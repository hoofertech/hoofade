from typing import List, Dict, Optional
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from models.trade import Trade
from models.message import Message
from models.db_trade import DBTrade
from sources.base import TradeSource
from sinks.base import MessageSink
from formatters.trade import TradeFormatter
from models.position import Position, InstrumentType
from services.trade_processor import (
    TradeProcessor,
    ProcessingResult,
    ProfitTaker,
)
import logging


logger = logging.getLogger(__name__)


class TradeService:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: AsyncSession,
        formatter: TradeFormatter,
        position_service=None,  # Inject position service
    ):
        self.sources = sources
        self.sinks = sinks
        self.db = db
        self.formatter = formatter
        self.position_service = position_service

    async def get_new_trades(self) -> List[ProcessingResult]:
        """Get and process new trades from all sources."""
        all_trades = []
        saved_trades = []

        # First collect all trades and positions from sources
        positions_by_key = {}  # Store merged positions by instrument key

        # Get and merge positions from all sources
        for source_id, source in self.sources.items():
            logger.debug(f"Processing positions from {source.source_id}")
            for position in source.get_positions():
                # Skip positions with zero quantity
                if position.quantity == 0:
                    continue

                key = self._get_position_key(position)
                if key in positions_by_key:
                    # Merge with existing position
                    existing = positions_by_key[key]
                    total_quantity = existing.quantity + position.quantity

                    # Skip if merged position would have zero quantity
                    if total_quantity == 0:
                        del positions_by_key[key]
                        continue

                    # Calculate weighted average cost basis
                    weighted_cost = (
                        existing.quantity * existing.cost_basis
                        + position.quantity * position.cost_basis
                    ) / total_quantity

                    positions_by_key[key] = Position(
                        instrument=position.instrument,
                        quantity=total_quantity,
                        cost_basis=weighted_cost,
                        market_price=position.market_price,  # Use latest mark price
                    )
                else:
                    # New position
                    positions_by_key[key] = position

            # Get trades
            for trade in source.get_last_day_trades():
                if not await self._is_trade_published(trade):
                    await self._save_trade(trade)
                    saved_trades.append(trade)
                    all_trades.append(trade)

        if not all_trades:
            return []

        # Use merged positions for processing
        merged_positions = list(positions_by_key.values())

        # Process trades through pipeline
        processor = TradeProcessor(merged_positions)
        processed_results, portfolio_matches = processor.process_trades(all_trades)

        return processed_results

    def _get_position_key(self, position: Position) -> str:
        """Generate a unique key for a position based on instrument details."""
        instrument = position.instrument
        if instrument.type == InstrumentType.OPTION and instrument.option_details:
            return (
                f"{instrument.symbol}_{instrument.option_details.expiry}_"
                f"{instrument.option_details.strike}_{instrument.option_details.option_type}"
            )
        return instrument.symbol

    def _apply_portfolio_match(
        self, match: ProfitTaker, positions: List[Position]
    ) -> bool:
        """
        Apply a portfolio match to a list of positions.
        Returns True if match was successfully applied.

        Args:
            match: ProfitTaker containing the buy and sell trades
            positions: List of positions to search for matches

        Returns:
            bool: True if match was successfully applied to a position
        """
        # Determine which side is from position (has no trades)
        if not match.buy_trade.trades:
            position_trade = match.buy_trade
            logger.debug(
                f"Buy side is from position: {position_trade.instrument.symbol}"
            )
        elif not match.sell_trade.trades:
            position_trade = match.sell_trade
            logger.debug(
                f"Sell side is from position: {position_trade.instrument.symbol}"
            )
        else:
            logger.debug("No position trades found in profit taker")
            return False

        # Find matching position
        for position in positions:
            if position.instrument == position_trade.instrument:
                # Update position quantity
                position.quantity += position_trade.quantity
                logger.debug(
                    f"Applied position match for {position.instrument.symbol}: "
                    f"updated quantity from {position.quantity + position_trade.quantity} "
                    f"to {position.quantity}"
                )
                return True

        return False

    async def publish_trades(self, trades: List[ProcessingResult]) -> bool:
        """Publish processed trades to all sinks."""
        if not trades:
            return True

        # Get timestamp of most recent trade
        last_trade_timestamp = max(trade.timestamp for trade in trades)
        date_str = last_trade_timestamp.strftime("%d %b %Y %H:%M").upper()
        # Format trades into messages
        content = [
            f"Trades on {date_str}",
            "",  # Empty line after header
        ]

        for msg in self.formatter.format_trades(trades):
            content.append(msg.content)

        # Create combined message
        combined_message = Message(
            content="\n".join(content),
            timestamp=datetime.now(timezone.utc),
            metadata={"type": "trade_batch"},
        )

        publish_success = True
        # Publish to all sinks
        for sink in self.sinks.values():
            if sink.can_publish():
                if await sink.publish(combined_message):
                    logger.debug(f"Published {len(trades)} trades to {sink.sink_id}")
                else:
                    logger.warning(f"Failed to publish trades to {sink.sink_id}")
                    publish_success = False

        return publish_success

    async def _is_trade_published(self, trade: Trade) -> bool:
        """Check if a trade has already been published."""
        query = select(DBTrade).where(
            DBTrade.trade_id == trade.trade_id, DBTrade.source_id == trade.source_id
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none() is not None

    async def _save_trade(
        self, trade: Trade, matching_trade: Optional[DBTrade] = None
    ) -> None:
        """Save a trade to the database and update matching trade if exists."""
        try:
            # Convert domain trade to DB model
            db_trade = DBTrade.from_domain(trade)

            # Set matched flag if there's a matching trade
            if matching_trade:
                # Update both trades' matched status in a single transaction
                stmt = (
                    update(DBTrade)
                    .where(
                        DBTrade.trade_id.in_([trade.trade_id, matching_trade.trade_id])
                    )
                    .values(matched=True)
                )
                await self.db.execute(stmt)

            # Add new trade to session
            self.db.add(db_trade)

            # Commit changes
            await self.db.commit()

        except Exception as e:
            logger.error(f"Error saving trade: {str(e)}")
            await self.db.rollback()
            raise e

    async def get_merged_positions(self) -> List[Position]:
        """Get merged positions from all sources."""
        positions_by_key = {}  # Store merged positions by instrument key

        # Get and merge positions from all sources
        for source_id, source in self.sources.items():
            logger.debug(f"Processing positions from {source.source_id}")
            for position in source.get_positions():
                # Skip positions with zero quantity
                if position.quantity == 0:
                    continue

                key = self._get_position_key(position)
                if key in positions_by_key:
                    # Merge with existing position
                    existing = positions_by_key[key]
                    total_quantity = existing.quantity + position.quantity

                    # Skip if merged position would have zero quantity
                    if total_quantity == 0:
                        del positions_by_key[key]
                        continue

                    # Calculate weighted average cost basis
                    weighted_cost = (
                        existing.quantity * existing.cost_basis
                        + position.quantity * position.cost_basis
                    ) / total_quantity

                    positions_by_key[key] = Position(
                        instrument=position.instrument,
                        quantity=total_quantity,
                        cost_basis=weighted_cost,
                        market_price=position.market_price,  # Use latest mark price
                    )
                else:
                    # New position
                    positions_by_key[key] = position

        return list(positions_by_key.values())
