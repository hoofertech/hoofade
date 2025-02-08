import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import default_timezone
from formatters.trade import TradeFormatter
from models.db_trade import DBTrade
from models.message import Message
from models.position import Position
from models.trade import Trade
from services.position_service import PositionService
from services.trade_processor import (
    ProcessingResult,
    ProfitTaker,
    TradeProcessor,
)
from sinks.base import MessageSink
from sources.base import TradeSource

logger = logging.getLogger(__name__)


class TradeService:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: AsyncSession,
        formatter: TradeFormatter,
        position_service: PositionService,
    ):
        self.sources = sources
        self.sinks = sinks
        self.db = db
        self.formatter = formatter
        self.position_service = position_service

    async def get_new_trades(
        self,
    ) -> Tuple[List[ProcessingResult], List[ProfitTaker]]:
        """Get and process new trades from all sources."""
        all_trades = []
        saved_trades = []

        # Get and merge trades from all sources
        for source_id, source in self.sources.items():
            # Get trades
            for trade in source.get_last_day_trades():
                if not await self._is_trade_published(trade):
                    await self._save_trade(trade)
                    saved_trades.append(trade)
                    all_trades.append(trade)

        if not all_trades:
            return [], []

        # Process trades through pipeline
        processor = TradeProcessor(self.position_service.merged_positions)
        processed_results, portfolio_matches = processor.process_trades(all_trades)

        return processed_results, portfolio_matches

    def _apply_portfolio_match(self, match: ProfitTaker, positions: List[Position]) -> bool:
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
            logger.debug(f"Buy side is from position: {position_trade.instrument.symbol}")
        elif not match.sell_trade.trades:
            position_trade = match.sell_trade
            logger.debug(f"Sell side is from position: {position_trade.instrument.symbol}")
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
            timestamp=datetime.now(default_timezone()),
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
        query = select(DBTrade).where(DBTrade.trade_id == trade.trade_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none() is not None

    async def _save_trade(self, trade: Trade, matching_trade: Optional[DBTrade] = None) -> None:
        """Save a trade to the database and update matching trade if exists."""
        try:
            # Convert domain trade to DB model
            db_trade = DBTrade.from_domain(trade)

            # Set matched flag if there's a matching trade
            if matching_trade:
                # Update both trades' matched status in a single transaction
                stmt = (
                    update(DBTrade)
                    .where(DBTrade.trade_id.in_([trade.trade_id, matching_trade.trade_id]))
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
