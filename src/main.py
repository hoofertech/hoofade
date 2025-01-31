import asyncio
import logging
from models.trade import Trade
from models.db_trade import DBTrade
from formatters.trade import TradeFormatter
from typing import Dict
from models.db_trade import Base
from sources.ibkr import IBKRSource
from sinks.twitter import TwitterSink
from models.instrument import InstrumentType
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from config import get_source_configs, get_sink_configs, get_db_url
from sources.base import TradeSource
from sinks.base import MessageSink
from sqlalchemy import select
from typing import Optional
from dotenv import load_dotenv
from sources.ibkr_json_source import JsonSource
from sinks.cli import CLISink
from formatters.portfolio import PortfolioFormatter
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def create_sources() -> Dict[str, TradeSource]:
    """Create trade sources from configuration"""
    sources = {}
    configs = get_source_configs()

    for source_id, config in configs.items():
        if config["type"] == "ibkr":
            logger.info(f"Creating IBKR source {source_id}")
            sources[source_id] = IBKRSource(
                source_id=config["source_id"],
                portfolio_token=config["portfolio"]["token"],
                portfolio_query_id=config["portfolio"]["query_id"],
                trades_token=config["trades"]["token"],
                trades_query_id=config["trades"]["query_id"],
                save_dir=config.get("save_dir", None),
            )
        elif config["type"] == "json":
            logger.info(f"Creating JSON source {source_id}")
            sources[source_id] = JsonSource(
                source_id=config["source_id"],
                data_dir=config.get("data_dir", "data/flex_reports"),
            )

    return sources


def create_sinks() -> Dict[str, MessageSink]:
    """Create message sinks from configuration"""
    sinks = {}
    configs = get_sink_configs()

    for sink_id, config in configs.items():
        if config["type"] == "twitter":
            logger.info(f"Creating Twitter sink {sink_id}")
            sinks[sink_id] = TwitterSink(
                sink_id=config["sink_id"],
                bearer_token=config["bearer_token"],
                api_key=config["api_key"],
                api_secret=config["api_secret"],
                access_token=config["access_token"],
                access_token_secret=config["access_token_secret"],
            )
        elif config["type"] == "cli":
            logger.info(f"Creating CLI sink {sink_id}")
            sinks[sink_id] = CLISink(sink_id=config["sink_id"])

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
        self.portfolio_formatter = PortfolioFormatter()
        self.last_portfolio_post = None

    async def publish_portfolio(self, source: TradeSource):
        """Publish portfolio from a source"""
        positions = []
        async for position in source.get_positions():
            positions.append(position)

        timestamp = datetime.now(timezone.utc)
        message = self.portfolio_formatter.format_portfolio(positions, timestamp)

        for sink in self.sinks.values():
            if sink.can_publish():
                await sink.publish(message)

        self.last_portfolio_post = timestamp

    def should_post_portfolio(self) -> bool:
        now = datetime.now(timezone.utc)

        # Post if we've never posted before
        if self.last_portfolio_post is None:
            return True

        # Post if it's a new UTC day
        last_post_day = self.last_portfolio_post.date()
        current_day = now.date()
        return current_day > last_post_day

    async def process_trades(self):
        try:
            for source in self.sources.values():
                logger.debug(f"Processing trades from {source.source_id}")
                async for trade in source.get_last_day_trades():
                    logger.debug(
                        f"Processing trade {trade.trade_id} from {source.source_id}"
                    )
                    await self.process_single_trade(trade)

            await self.db.commit()
        except Exception as e:
            logger.error(f"Error processing trades: {str(e)}")
            await self.db.rollback()
            raise e

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
                    logger.debug(f"Published trade {trade.trade_id} to {sink.sink_id}")
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
        query = select(DBTrade).where(
            DBTrade.symbol == trade.instrument.symbol,
            DBTrade.instrument_type == trade.instrument.type,
            DBTrade.quantity == -trade.quantity,  # Opposite quantity
            DBTrade.matched == False,  # Not already matched # noqa: E712
            DBTrade.source_id == trade.source_id,  # Same source
        )

        if trade.instrument.type == InstrumentType.OPTION:
            if not trade.instrument.option_details:
                raise ValueError("Missing option details for option trade")

            query = query.where(
                DBTrade.option_type == trade.instrument.option_details.option_type,
                DBTrade.strike == trade.instrument.option_details.strike,
                DBTrade.expiry == trade.instrument.option_details.expiry,
            )

        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def run(self):
        """Main loop to process trades periodically"""
        json_only_sources = all(
            isinstance(src, JsonSource) for src in self.sources.values()
        )

        while True:
            for source in self.sources.values():
                try:
                    if not await source.connect():
                        logger.error(f"Failed to connect to source {source.source_id}")
                        continue

                    # Check if we should post portfolio
                    if self.should_post_portfolio():
                        logger.info(
                            f"Publishing portfolio from source {source.source_id}"
                        )
                        await self.publish_portfolio(source)

                    logger.info(f"Processing trades from source {source.source_id}")
                    await self.process_trades()

                finally:
                    await source.disconnect()

            if json_only_sources:
                break
            await asyncio.sleep(900)  # Sleep for 15 minutes


async def create_db() -> AsyncSession:
    logger.info("Creating database session: %s", get_db_url())
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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    load_dotenv(override=True)
    asyncio.run(main())
