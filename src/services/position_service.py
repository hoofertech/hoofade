from typing import Dict, Optional, cast, List
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.db_portfolio import DBPortfolio
from sources.base import TradeSource
from formatters.portfolio import PortfolioFormatter
from sinks.base import MessageSink
from models.position import Position
from sqlalchemy import text
from datetime import timezone
import logging

logger = logging.getLogger(__name__)


class PositionService:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: AsyncSession,
    ):
        self.sources = sources
        self.sinks = sinks
        self.db = db
        self.portfolio_formatter = PortfolioFormatter()

    async def publish_portfolio(self, positions: List[Position], timestamp: datetime):
        message = self.portfolio_formatter.format_portfolio(positions, timestamp)

        publish_success = True
        for sink in self.sinks.values():
            if sink.can_publish():
                if not await sink.publish(message):
                    publish_success = False
                    logger.error(f"Failed to publish portfolio to {sink.sink_id}")

        if publish_success:
            # Save portfolio post for all sources since it's consolidated
            await self._save_portfolio_post(timestamp)
            logger.info("Successfully published consolidated portfolio")

    async def should_post_portfolio(self, now: datetime) -> bool:
        """Check if we should post portfolio based on last post time"""
        last_post = await self._get_last_portfolio_post("all")
        logger.info(f"Last portfolio post: {last_post}")
        if last_post is None:
            return True

        last_post_day = last_post.date()
        current_day = now.date()
        logger.info(
            f"Last post day: {last_post_day}, current day: {current_day}: {"should post" if current_day > last_post_day else "should not post"}"
        )
        return current_day > last_post_day

    async def _get_last_portfolio_post(self, source_id: str) -> Optional[datetime]:
        """Get the last portfolio post timestamp from DB"""
        try:
            query = select(DBPortfolio).where(DBPortfolio.source_id == source_id)
            result = await self.db.execute(query)
            record = result.scalar_one_or_none()

            if record is None:
                logger.warning(f"No portfolio post found for source_id: {source_id}")
                return None

            # Ensure the timestamp is timezone-aware
            if record.last_post.tzinfo is None:
                record.last_post = record.last_post.replace(tzinfo=timezone.utc)

            return cast(datetime, record.last_post)
        except Exception as e:
            logger.error(f"Error getting last portfolio post: {str(e)}")
            return None

    async def _save_portfolio_post(self, timestamp: datetime):
        """Save or update the last portfolio post timestamp"""
        try:
            portfolio = DBPortfolio(last_post=timestamp, source_id="all")
            await self.db.merge(portfolio)
            await self.db.commit()
        except Exception as e:
            logger.error(f"Error saving portfolio post: {str(e)}")
            await self.db.rollback()
