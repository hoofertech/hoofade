import asyncio
import logging
import uvicorn
import threading
from formatters.trade import TradeFormatter
from typing import Dict
from models.db_trade import Base
from sources.ibkr import IBKRSource
from sinks.twitter import TwitterSink
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker
from config import get_source_configs, get_sink_configs, get_db_url, get_web_config
from sources.base import TradeSource
from sinks.base import MessageSink
from dotenv import load_dotenv
from sources.ibkr_json_source import JsonSource
from sinks.cli import CLISink
from services.position_service import PositionService
from services.trade_service import TradeService
from datetime import datetime, timezone
from sinks.database import DatabaseSink
from web.server import app, init_app

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


def create_sinks(
    async_session: async_sessionmaker[AsyncSession],
) -> Dict[str, MessageSink]:
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
        elif config["type"] == "cli":
            sinks[sink_id] = CLISink(sink_id=config["sink_id"])
        elif config["type"] == "database":
            sinks[sink_id] = DatabaseSink(
                sink_id=config["sink_id"], async_session=async_session
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
        self.position_service = PositionService(sources, sinks, db)
        self.trade_service = TradeService(
            sources, sinks, db, formatter, self.position_service
        )
        self.sources = sources

    async def run(self):
        """Main loop to process trades periodically"""
        first_run = True
        now = datetime.now(timezone.utc)
        while True:
            all_sources_done = False
            max_sleep = 0

            for source in self.sources.values():
                # Process trades
                if not await source.load_last_day_trades():
                    logger.error(f"Failed to load trades for source {source.source_id}")
                    continue
                logger.info(f"Loading new trades for source {source.source_id}")
                new_trades = await self.trade_service.get_new_trades()
                logger.info(
                    f"Loaded {len(new_trades)} trades for source {source.source_id}"
                )

                if new_trades:
                    now = min(trade.timestamp for trade in new_trades)
                    logger.info(f">>> Newest trade timestamp: {now}")
                else:
                    logger.info(
                        f">>> No new trades for source {source.source_id}: {now}"
                    )

                # Check if we should post portfolio
                should_post = await self.position_service.should_post_portfolio(
                    source.source_id, now
                )

                if should_post:
                    if not await source.load_positions():
                        logger.error(f"Failed to connect to source {source.source_id}")
                        continue
                    await self.position_service.publish_portfolio(source, now)
                else:
                    if first_run:
                        if not await source.load_positions():
                            logger.error(
                                f"Failed to connect to source {source.source_id}"
                            )
                            continue
                    logger.info(f"Skipping portfolio for source {source.source_id}")

                logger.info(
                    f"Publishing {len(new_trades)} trades for source {source.source_id}"
                )
                await self.trade_service.publish_trades(new_trades)
                logger.info(
                    f"Published {len(new_trades)} trades for source {source.source_id}"
                )

                all_sources_done = all_sources_done or source.is_done()
                max_sleep = max(max_sleep, source.get_sleep_time())

            if all_sources_done:
                logger.info("All sources are done, exiting")
                break
            first_run = False
            logger.info(f"Sleeping for {max_sleep} seconds")
            await asyncio.sleep(max_sleep)


async def create_db_maker() -> async_sessionmaker[AsyncSession]:
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
    return async_session


async def run_web_server():
    web_config = get_web_config()
    config = uvicorn.Config(
        app,
        host=web_config["host"],
        port=web_config["port"],
        log_level=web_config["log_level"],
    )
    server = uvicorn.Server(config)
    logger.info(f"Starting web server on {web_config['host']}:{web_config['port']}")
    await server.serve()


async def run_trade_publisher(sources, sinks, db, formatter):
    try:
        publisher = TradePublisher(sources, sinks, db, formatter)
        await publisher.run()
    finally:
        await db.close()


def start_web_server(db_maker):
    init_app(db_maker)
    asyncio.run(run_web_server())


async def main():
    logging.basicConfig(level=logging.INFO)

    sources = create_sources()
    db_maker = await create_db_maker()
    sinks = create_sinks(db_maker)
    formatter = TradeFormatter()
    db = db_maker()

    # Start web server in a separate thread
    web_thread = threading.Thread(
        target=start_web_server, args=(db_maker,), daemon=True
    )
    web_thread.start()

    # Run trade publisher in main thread
    await run_trade_publisher(sources, sinks, db, formatter)

    # Keep the program running
    try:
        while True:
            await asyncio.sleep(60)  # Sleep for a minute
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Cleanup code here if needed
        await db.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    load_dotenv(override=True)
    asyncio.run(main())
