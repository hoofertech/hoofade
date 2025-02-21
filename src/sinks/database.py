import copy
import logging
from datetime import datetime, timedelta
from typing import List

from database import Database
from models.position import Position
from models.trade import Trade
from services.position_service import PositionService
from services.trade_bucket_manager import TradeBucketManager
from services.trade_processor import TradeProcessor
from utils.datetime_utils import format_datetime

from .base import MessageSink

logger = logging.getLogger(__name__)


class DatabaseSink(MessageSink):
    def __init__(self, sink_id: str, db: Database):
        super().__init__(sink_id)
        self.db = db
        self.bucket_manager = TradeBucketManager()
        self.positions = []

    async def publish_trades(self, trades: List[Trade], now: datetime) -> bool:
        try:
            logger.info(f"Publishing {len(trades)} trades at {format_datetime(now)}")
            if trades:
                self.bucket_manager.add_trades(trades)

            # Get completed buckets for all granularities
            completed_buckets = self.bucket_manager.get_completed_buckets(now)

            # Process and save completed buckets
            for granularity, buckets in completed_buckets.items():
                for bucket_trades in buckets:
                    start_time = self.bucket_manager._round_time_down(
                        bucket_trades[0].timestamp, self.bucket_manager.intervals[granularity]
                    )
                    end_time = start_time + self.bucket_manager.intervals[granularity]
                    await self._create_and_save_message(
                        bucket_trades, start_time, end_time, granularity
                    )

            for bucket_trades in completed_buckets["15m"]:
                new_trades = bucket_trades
                if new_trades:
                    logger.info(f"Applying {len(new_trades)} trades to portfolio")
                for new_trade in new_trades:
                    await PositionService.apply_new_trade(new_trade, self.positions)

                logger.info(f"Published {len(new_trades)} trades.")
                if self.PUBLISH_PORTFOLIO_AFTER_EACH_TRADE:
                    last_trade_timestamp = max(trade.timestamp for trade in new_trades)
                    await self.publish_portfolio(
                        self.positions, last_trade_timestamp + timedelta(seconds=1)
                    )

            return True
        except Exception as e:
            logger.error(f"Error in database sink: {str(e)}", exc_info=True)
            return False

    async def publish_portfolio(self, positions: List[Position], now: datetime) -> bool:
        try:
            await self.db.save_portfolio_message(now, positions)
            return True
        except Exception as e:
            logger.error(f"Error saving portfolio: {str(e)}", exc_info=True)
            return False

    async def _create_and_save_message(
        self, trades: List[Trade], start_time: datetime, end_time: datetime, granularity: str
    ) -> None:
        """Process trades and create/save message"""
        # Process trades to get combined trades and profit takers
        processor = TradeProcessor(self.positions)
        processed_results, _ = processor.process_trades(trades)
        max_timestamp = max(trade.timestamp for trade in trades)

        # Save to database with both raw and processed trades
        await self.db.save_trade_message(
            {
                "id": f"{format_datetime(start_time)}_{granularity}",
                "timestamp": format_datetime(max_timestamp),
                "granularity": granularity,
                "metadata": {
                    "type": "trd",
                    "granularity": granularity,
                    "interval_start": format_datetime(start_time),
                    "interval_end": format_datetime(end_time),
                },
                "trades": trades,
                "processed_trades": processed_results,
            }
        )

    def update_portfolio(self, positions: List[Position]) -> bool:
        self.positions = copy.deepcopy(positions)
        return True
