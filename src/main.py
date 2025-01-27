import time
import schedule
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict
from sqlalchemy.orm import Session

from src.sources.base import TradeSource
from src.sinks.base import MessageSink
from src.formatters.trade import TradeFormatter
from src.sources.factory import SourceFactory
from src.sinks.factory import SinkFactory
from src.config import get_source_configs, get_sink_configs, get_db_session
from src.models.trade import Trade as DBTrade

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TradePublisher:
    def __init__(self):
        self.sources: Dict[str, TradeSource] = {}
        self.sinks: Dict[str, MessageSink] = {}
        self.formatter = TradeFormatter()
        self.db: Session = get_db_session()
        self.setup_sources()
        self.setup_sinks()

    def setup_sources(self):
        source_configs = get_source_configs()
        for source_id, config in source_configs.items():
            try:
                source = SourceFactory.create_source(config["type"], config)
                if source.connect():
                    self.sources[source_id] = source
                    logger.info(f"Connected to source: {source_id}")
                else:
                    logger.error(f"Failed to connect to source: {source_id}")
            except Exception as e:
                logger.error(f"Error setting up source {source_id}: {str(e)}")

    def setup_sinks(self):
        sink_configs = get_sink_configs()
        for sink_id, config in sink_configs.items():
            try:
                sink = SinkFactory.create_sink(config["type"], config)
                if sink.connect():
                    self.sinks[sink_id] = sink
                    logger.info(f"Connected to sink: {sink_id}")
                else:
                    logger.error(f"Failed to connect to sink: {sink_id}")
            except Exception as e:
                logger.error(f"Error setting up sink {sink_id}: {str(e)}")

    def process_trades(self):
        try:
            since = datetime.now(timezone.utc) - timedelta(minutes=15)

            for source in self.sources.values():
                for trade in source.get_recent_trades(since):
                    self.process_single_trade(trade)

            self.db.commit()
        except Exception as e:
            logger.error(f"Error processing trades: {str(e)}")
            self.db.rollback()

    def process_single_trade(self, trade):
        # Save to database
        db_trade = DBTrade(
            symbol=trade.symbol,
            quantity=float(trade.quantity),
            price=float(trade.price),
            side=trade.side,
            timestamp=trade.timestamp,
            source_id=trade.source_id,
            trade_id=trade.trade_id,
        )
        self.db.add(db_trade)

        # Find matching trade
        matching_trade = self.find_matching_trade(trade)

        # Format message
        message = self.formatter.format_trade(trade, matching_trade)

        # Publish to all sinks
        for sink in self.sinks.values():
            if sink.can_publish():
                if sink.publish(message):
                    logger.info(f"Published trade {trade.trade_id} to {sink.sink_id}")
                else:
                    logger.warning(
                        f"Failed to publish trade {trade.trade_id} to {sink.sink_id}"
                    )

        # Update matching trade if exists
        if matching_trade:
            matching_trade.matched = True
            self.db.add(matching_trade)

    def find_matching_trade(self, trade):
        return (
            self.db.query(DBTrade)
            .filter(
                DBTrade.symbol == trade.symbol,
                DBTrade.side != trade.side,
                not DBTrade.matched,
            )
            .first()
        )

    def cleanup(self):
        for source in self.sources.values():
            try:
                source.disconnect()
            except Exception as e:
                logger.error(
                    f"Error disconnecting from source {source.source_id}: {str(e)}"
                )


def main():
    publisher = TradePublisher()

    try:
        schedule.every(15).minutes.do(publisher.process_trades)
        logger.info("Trade publisher started")

        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        publisher.cleanup()


if __name__ == "__main__":
    main()
