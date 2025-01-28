import asyncio
from datetime import datetime, timezone, timedelta
import logging
from models.trade import Trade
from models.db_trade import DBTrade
from formatters.trade import TradeFormatter
from typing import Dict
from models.db_trade import Base
from sources.ibkr import IBKRSource
from sinks.twitter import TwitterSink
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from config import get_source_configs, get_sink_configs, get_db_url
from sources.base import TradeSource
from sinks.base import MessageSink
from sqlalchemy import select
from typing import Optional


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_sources() -> Dict[str, TradeSource]:
    """Create trade sources from configuration"""
    sources = {}
    configs = get_source_configs()

    for source_id, config in configs.items():
        if config["type"] == "ibkr":
            sources[source_id] = IBKRSource(
                source_id=config["source_id"],
                portfolio_token=config["portfolio"]["token"],
                portfolio_query_id=config["portfolio"]["query_id"],
                trades_token=config["trades"]["token"],
                trades_query_id=config["trades"]["query_id"],
            )

    return sources


def create_sinks() -> Dict[str, MessageSink]:
    """Create message sinks from configuration"""
    sinks = {}
    configs = get_sink_configs()

    for sink_id, config in configs.items():
        if config["type"] == "twitter":
            sinks[sink_id] = TwitterSink(
                sink_id=config["sink_id"],
                bearer_token=config["bearer_token"],
                api_key=config["api_key"],
                api_secret=config["api_secret"],
                access_token=config["access_token"],
                access_token_secret=config["access_token_secret"],
            )

    return sinks


class TradePublisher:
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

    async def process_trades(self):
        try:
            since = datetime.now(timezone.utc) - timedelta(minutes=15)

            for source in self.sources.values():
                async for trade in source.get_recent_trades(since):
                    await self.process_single_trade(trade)

            await self.db.commit()
        except Exception as e:
            logger.error(f"Error processing trades: {str(e)}")
            await self.db.rollback()

    async def process_single_trade(self, trade):
        # Save to database using the conversion method
        db_trade = DBTrade.from_domain(trade)
        self.db.add(db_trade)

        # Find matching trade
        matching_db_trade = await self.find_matching_trade(trade)

        # Format message with domain model
        matching_trade = matching_db_trade.to_domain() if matching_db_trade else None
        message = self.formatter.format_trade(trade, matching_trade)

        # Publish to all sinks
        for sink in self.sinks.values():
            if sink.can_publish():
                if await sink.publish(message):
                    logger.info(f"Published trade {trade.trade_id} to {sink.sink_id}")
                else:
                    logger.warning(
                        f"Failed to publish trade {trade.trade_id} to {sink.sink_id}"
                    )

        # Update matching trade if exists
        if matching_db_trade:
            setattr(matching_db_trade, "matched", True)
            self.db.add(matching_db_trade)

    async def find_matching_trade(self, trade: Trade) -> Optional[DBTrade]:
        """Find a matching trade for the given trade"""
        result = await self.db.execute(
            select(DBTrade).where(
                DBTrade.symbol == trade.symbol,
                DBTrade.quantity == -trade.quantity,  # Opposite quantity
                DBTrade.matched == False,  # Not already matched # noqa: E712
                DBTrade.source_id == trade.source_id,  # Same source
            )
        )
        return result.scalar_one_or_none()

    async def run(self):
        """Main loop to process trades periodically"""
        while True:
            for source in self.sources.values():
                try:
                    if not await source.connect():
                        logger.error(f"Failed to connect to source {source.source_id}")
                        continue

                    await self.process_trades()

                finally:
                    await source.disconnect()

            await asyncio.sleep(900)  # Sleep for 15 minutes


async def create_db() -> AsyncSession:
    """Create database session"""
    engine = create_async_engine(get_db_url())

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Use async_sessionmaker instead of sessionmaker
    async_session = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # Create and return a new session
    return async_session()


async def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Load configuration and create components
    sources = create_sources()
    sinks = create_sinks()
    db = await create_db()  # Add await here
    formatter = TradeFormatter()

    # Create and run publisher
    publisher = TradePublisher(sources, sinks, db, formatter)
    await publisher.run()
