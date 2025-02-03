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

from services.trade_processor import (
    TradeProcessor,
    ProcessingResult,
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

        # 1. Collect and save all new trades from sources
        for source in self.sources.values():
            logger.debug(f"Processing trades from {source.source_id}")
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
                positions.extend(await self.position_service.get_positions(source))

        # 3. Process trades through pipeline
        processor = TradeProcessor(positions)
        processed_results = processor.process_trades(all_trades)

        return processed_results

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
