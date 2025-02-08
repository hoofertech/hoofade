import asyncio
import logging
import threading
from datetime import datetime
from typing import Dict

import uvicorn
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config import (
    default_timezone,
    get_db_url,
    get_sink_configs,
    get_source_configs,
    get_web_config,
)
from formatters.trade import TradeFormatter
from models.db_trade import Base
from services.position_service import PositionService
from services.trade_service import TradeService
from sinks.base import MessageSink
from sinks.cli import CLISink
from sinks.database import DatabaseSink
from sinks.twitter import TwitterSink
from sources.base import TradeSource
from sources.ibkr import IBKRSource
from sources.ibkr_json_source import JsonSource
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
            sinks[sink_id] = DatabaseSink(sink_id=config["sink_id"], async_session=async_session)

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
        self.trade_service = TradeService(sources, sinks, db, formatter, self.position_service)
        self.sources = sources

    async def run(self):
        """Main loop to process trades periodically"""
        now = datetime.now(default_timezone())

        while True:
            all_sources_done = False
            max_sleep = 0
            new_trades = []

            # First process trades for all sources to get correct timestamp
            for source in self.sources.values():
                if not await source.load_last_day_trades():
                    logger.error(f"Failed to load trades for source {source.source_id}")
                    continue

            # Load and merge positions if needed
            if await self.position_service.should_post_portfolio(now):
                # Load positions from all sources
                for source in self.sources.values():
                    if not await source.load_positions():
                        logger.error(f"Failed to connect to source {source.source_id}")
                        continue

                # Use TradeService's position merging logic
                self.position_service.merged_positions = (
                    await self.position_service.get_merged_positions()
                )
                if self.position_service.merged_positions:
                    # Use the latest report time from any position
                    latest_report_time = max(
                        (
                            p.report_time
                            for p in self.position_service.merged_positions
                            if p.report_time is not None
                        ),
                        default=None,
                    )
                    if latest_report_time:
                        now = latest_report_time
                        logger.info(f">>> Using latest position report time: {now}")

            (
                new_trades,
                portfolio_profit_takers,
            ) = await self.trade_service.get_new_trades()
            logger.info(f"Loaded {len(new_trades)} trades")

            if new_trades:
                now = max(now, max(trade.timestamp for trade in new_trades))
                logger.info(f">>> Newest trade timestamp: {now}")
            else:
                logger.info(f">>> No new trades: {now}")

            if portfolio_profit_takers:
                logger.info(f"Applying {len(portfolio_profit_takers)} portfolio profit takers")
                for profit_taker in portfolio_profit_takers:
                    if await self.position_service.apply_profit_taker(
                        profit_taker, self.position_service.merged_positions
                    ):
                        logger.info(
                            f"Applied portfolio profit taker for {profit_taker.instrument.symbol}"
                        )

            # Check if we should publish with updated timestamp
            if await self.position_service.should_post_portfolio(now):
                await self.position_service.publish_portfolio(
                    self.position_service.merged_positions, now
                )

            # Now publish trades
            if new_trades:
                logger.info(f"Publishing {len(new_trades)} trades.")
                await self.trade_service.publish_trades(new_trades)
                logger.info(f"Published {len(new_trades)} trades.")

            # Check if all sources are done
            for source in self.sources.values():
                all_sources_done = all_sources_done or source.is_done()
                max_sleep = max(max_sleep, source.get_sleep_time())

            if all_sources_done:
                logger.info("All sources are done, exiting")
                break

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
    web_thread = threading.Thread(target=start_web_server, args=(db_maker,), daemon=True)
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
