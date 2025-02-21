import asyncio
import logging
import threading
from datetime import timedelta
from typing import Dict

import uvicorn
from dotenv import load_dotenv

from config import (
    get_sink_configs,
    get_source_configs,
    get_web_config,
)
from database import Database, create_db
from formatters.trade import TradeFormatter
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


async def create_sinks(db: Database) -> Dict[str, MessageSink]:
    sinks = {}
    configs = get_sink_configs()

    for sink_id, config in configs.items():
        if config["type"] == "twitter":
            sink = TwitterSink(
                sink_id=config["sink_id"],
                db=db,
                bearer_token=config["bearer_token"],
                api_key=config["api_key"],
                api_secret=config["api_secret"],
                access_token=config["access_token"],
                access_token_secret=config["access_token_secret"],
            )
        elif config["type"] == "cli":
            sink = CLISink(sink_id=config["sink_id"], db=db)
        elif config["type"] == "database":
            sink = DatabaseSink(sink_id=config["sink_id"], db=db)
        else:
            raise ValueError(f"Unknown sink type: {config['type']}")

        await sink.initialize()
        sinks[sink_id] = sink

    return sinks


class TradePublisher:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: Database,
        formatter: TradeFormatter,
    ):
        self.position_service = PositionService(sources, sinks, db)
        self.trade_service = TradeService(sources, sinks, db, formatter, self.position_service)
        self.sources = sources
        self.db = db

    async def run(self):
        """Main loop to process trades periodically"""
        now = None

        while True:
            all_sources_done = False
            max_sleep = 0
            new_trades = []

            # First process trades for all sources to get correct timestamp
            for source in self.sources.values():
                success, last_report_time = await source.load_last_day_trades()
                logger.info(f"Loaded {last_report_time} trades for {source.source_id}")
                if not success:
                    logger.error(f"Failed to load trades for source {source.source_id}")
                    continue

                if last_report_time is not None:
                    if now is None or last_report_time > now:
                        now = last_report_time - timedelta(seconds=1)
                        logger.info(f"Updating now to {now} after loading trades")

            should_post_portfolio = (
                now is not None and await self.position_service.should_post_portfolio(now)
            )
            # Load and merge positions if needed
            if should_post_portfolio:
                logger.info("Loading positions at %s", now)
                # Load positions from all sources
                for source in self.sources.values():
                    success, last_report_time = await source.load_positions()
                    if not success:
                        logger.error(f"Failed to connect to source {source.source_id}")
                    if last_report_time is not None:
                        if now is None or last_report_time > now:
                            logger.info(
                                f"Updating now to {last_report_time} after loading positions"
                            )
                            now = last_report_time

                # Use TradeService's position merging logic
                self.position_service.merged_positions = (
                    await self.position_service.get_merged_positions()
                )
            new_trades = await self.trade_service.get_new_trades()
            logger.info(f"Loaded {len(new_trades)} trades")

            if should_post_portfolio and now is not None:
                last_trade_timestamp = (
                    max(trade.timestamp for trade in new_trades) - timedelta(seconds=1)
                    if new_trades
                    else now
                )

                logger.info(f"  Last trade timestamp: {last_trade_timestamp}")

                # Remove any portfolio published with a greater timestamp
                await self.db.remove_future_portfolio_messages(last_trade_timestamp)
                logger.info("Flushing trades before reloading positions.")
                await self.trade_service.publish_trades_svc([], now)
                logger.info("Flushed trades.")
                await self.position_service.publish_portfolio_svc(
                    self.position_service.merged_positions, last_trade_timestamp, now
                )
                for sink in self.position_service.sinks.values():
                    sink.update_portfolio(self.position_service.merged_positions)

            if now is not None:
                logger.info(f"Publishing {len(new_trades)} trades.")
                await self.trade_service.publish_trades_svc(new_trades, now)
                logger.info(f"Published {len(new_trades)} trades.")
            else:
                logger.warning("No trades to published, as 'now' is not set")

            # Check if all sources are done
            for source in self.sources.values():
                all_sources_done = all_sources_done or source.is_done()
                max_sleep = max(max_sleep, source.get_sleep_time())

            if all_sources_done or not self.sources:
                logger.info("All sources are done, exiting")
                break

            logger.info(f"Sleeping for {max_sleep} seconds")
            await asyncio.sleep(max_sleep)


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


def start_web_server(db):
    init_app(db)
    asyncio.run(run_web_server())


async def main():
    logging.basicConfig(level=logging.INFO)

    sources = create_sources()
    db = await create_db()
    sinks = await create_sinks(db)
    formatter = TradeFormatter()

    # Start web server in a separate thread
    web_thread = threading.Thread(target=start_web_server, args=(db,), daemon=True)
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
    # Load environment variables
    load_dotenv(override=True)

    # Build static files before starting the app
    from build_static import build_static_files

    build_static_files()

    asyncio.run(main())
