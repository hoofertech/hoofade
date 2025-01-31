from typing import List, Dict, Tuple, Optional
from decimal import Decimal
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from models.trade import Trade
from models.message import Message
from models.db_trade import DBTrade
from models.instrument import InstrumentType
from sources.base import TradeSource
from sinks.base import MessageSink
from formatters.trade import TradeFormatter
import logging

logger = logging.getLogger(__name__)


class TradeService:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: AsyncSession,
        formatter: TradeFormatter,
    ):
        self.sources = sources
        self.sinks = sinks
        self.db = db
        self.formatter = formatter

    async def get_new_trades(self) -> List[Tuple[Trade, Optional[Trade]]]:
        """Get new trades from all sources with their matching trades if any."""
        new_trades = []
        for source in self.sources.values():
            logger.debug(f"Processing trades from {source.source_id}")
            async for trade in source.get_last_day_trades():
                if await self._is_trade_published(trade):
                    continue

                matching_trade = await self._find_matching_trade(trade)
                await self._save_trade(trade, matching_trade)

                new_trades.append(
                    (trade, matching_trade.to_domain() if matching_trade else None)
                )
        return new_trades

    async def publish_trades(self, trades: List[Tuple[Trade, Optional[Trade]]]) -> None:
        """Format and publish a batch of trades to all sinks."""
        if not trades:
            return

        # Get timestamp of most recent trade
        last_trade_timestamp = max(trade.timestamp for trade, _ in trades)
        date_str = last_trade_timestamp.strftime("%d %b %Y %H:%M").upper()

        # Format message content
        content = [
            f"Trades on {date_str}",
            "",  # Empty line after header
        ]

        for trade, matching_trade in trades:
            message = self.formatter.format_trade(trade, matching_trade)
            content.append(message.content)

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

    async def _find_matching_trade(self, trade: Trade) -> Optional[DBTrade]:
        """Find a matching trade that closes a position."""
        # Only look for matches if this is a closing trade
        if trade.quantity > 0:
            return None

        # Build query to find matching opening trade
        query = select(DBTrade).where(
            DBTrade.source_id == trade.source_id,
            DBTrade.symbol == trade.instrument.symbol,
            DBTrade.matched.is_(False),  # Use is_ instead of == False
        )

        # Add instrument-specific conditions
        if (
            trade.instrument.type == InstrumentType.OPTION
            and trade.instrument.option_details
        ):
            query = query.where(
                DBTrade.strike == trade.instrument.option_details.strike,
                DBTrade.expiry == trade.instrument.option_details.expiry,
                DBTrade.option_type == trade.instrument.option_details.option_type,
            )

        result = await self.db.execute(query)
        candidates = result.scalars().all()

        # Find the best matching trade (closest timestamp)
        best_match = None
        best_timestamp = None
        for candidate in candidates:
            # Convert SQLAlchemy Decimal to Python Decimal for comparison
            candidate_quantity = Decimal(str(candidate.quantity))
            trade_quantity = Decimal(str(trade.quantity))

            # Skip if quantities don't match
            if abs(candidate_quantity) != abs(trade_quantity):
                continue

            # Skip if signs are the same (both buy or both sell)
            if (candidate_quantity > 0) == (trade_quantity > 0):
                continue

            candidate_timestamp = candidate.timestamp
            if best_timestamp is None or str(candidate_timestamp) > str(best_timestamp):
                best_match = candidate
                best_timestamp = candidate_timestamp

        return best_match

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
