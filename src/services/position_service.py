from typing import Dict, Optional, cast
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.db_portfolio import DBPortfolio
from sources.base import TradeSource
from formatters.portfolio import PortfolioFormatter
from sinks.base import MessageSink
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

    async def publish_portfolio(self, source: TradeSource, timestamp: datetime):
        positions = source.get_positions()
        message = self.portfolio_formatter.format_portfolio(positions, timestamp)

        publish_success = True
        for sink in self.sinks.values():
            if sink.can_publish():
                if not await sink.publish(message):
                    publish_success = False
                    logger.error(f"Failed to publish portfolio to {sink.sink_id}")

        if publish_success:
            await self._save_portfolio_post(source.source_id, timestamp)
            logger.info(f"Successfully published portfolio for {source.source_id}")

    async def should_post_portfolio(self, source_id: str, now: datetime) -> bool:
        """Check if we should post portfolio based on last post time"""
        last_post = await self._get_last_portfolio_post(source_id)
        logger.info(f"Last portfolio post for {source_id}: {last_post}")
        if last_post is None:
            return True

        last_post_day = last_post.date()
        current_day = now.date()
        logger.info(f"Last post day: {last_post_day}, current day: {current_day}")
        return current_day > last_post_day

    async def _get_last_portfolio_post(self, source_id: str) -> Optional[datetime]:
        """Get the last portfolio post timestamp from DB"""
        query = select(DBPortfolio).where(DBPortfolio.source_id == source_id)
        result = await self.db.execute(query)
        record = result.scalar_one_or_none()
        if record is None:
            return None
        return cast(datetime, record.last_post)

    async def _save_portfolio_post(self, source_id: str, timestamp: datetime):
        """Save or update the last portfolio post timestamp"""
        try:
            portfolio = DBPortfolio(source_id=source_id, last_post=timestamp)
            await self.db.merge(portfolio)
            await self.db.commit()
        except Exception as e:
            logger.error(f"Error saving portfolio post: {str(e)}")
            await self.db.rollback()
