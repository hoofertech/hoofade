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
from models.position import Position
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
        source_positions = {}  # Store positions by source_id

        # First collect all trades and positions from sources
        for source_id, source in self.sources.items():
            logger.debug(f"Processing trades from {source.source_id}")
            # Get positions for each source
            positions = [pos for pos in source.get_positions()]
            source_positions[source_id] = positions

            # Get trades
            for trade in source.get_last_day_trades():
                if not await self._is_trade_published(trade):
                    await self._save_trade(trade)
                    saved_trades.append(trade)
                    all_trades.append(trade)

        if not all_trades:
            return []

        # 2. Get current portfolio positions
        positions = []
        if self.position_service:
            for source in self.sources.values():
                positions.extend(source.get_positions())

        # 3. Process trades through pipeline
        processor = TradeProcessor(positions)
        processed_results, portfolio_matches = processor.process_trades(all_trades)

        # Apply portfolio matches to source positions
        if portfolio_matches:
            for match in portfolio_matches:
                match_applied = False
                for source_id, positions in source_positions.items():
                    # Try to find and apply match to this source's positions
                    if self._apply_portfolio_match(match, positions):
                        match_applied = True
                        logger.info(f"Applied portfolio match to source {source_id}")
                        break

                if not match_applied:
                    logger.warning(
                        "Could not find matching position for portfolio match"
                    )

        return processed_results

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

    async def publish_trades(self, trades: List[ProcessingResult]) -> None:
        """Publish processed trades to all sinks."""
        if not trades:
            return

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

        # Publish to all sinks
        for sink in self.sinks.values():
            if sink.can_publish():
                if await sink.publish(combined_message):
                    logger.debug(f"Published {len(trades)} trades to {sink.sink_id}")
                else:
                    logger.warning(f"Failed to publish trades to {sink.sink_id}")

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
