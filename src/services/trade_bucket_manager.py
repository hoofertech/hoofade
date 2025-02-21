import copy
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from models.position import Position
from models.trade import Trade

logger = logging.getLogger(__name__)


class TradeBucketManager:
    def __init__(self):
        self.trade_buckets: Dict[str, List[Trade]] = {"15m": [], "1h": [], "1d": []}
        self.last_bucket_time: Dict[str, Optional[datetime]] = {
            "15m": None,
            "1h": None,
            "1d": None,
        }
        self.intervals = {
            "15m": timedelta(minutes=15),
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
        }
        self.positions: Dict[str, List[Position]] = {
            "15m": [],
            "1h": [],
            "1d": [],
        }

    def update_positions(self, positions: List[Position]) -> None:
        """Update positions for each granularity"""
        for granularity in self.positions.keys():
            self.positions[granularity] = copy.deepcopy(positions)

    def add_trades(self, trades: List[Trade]) -> None:
        """Add trades to appropriate time buckets"""
        logger.info(f">>> Adding {len(trades)} trades to buckets:")
        # for t in trades:
        #     logger.info(f"  {t.instrument} at {t.timestamp}")
        for trade in trades:
            for granularity in self.trade_buckets.keys():
                self.trade_buckets[granularity].append(trade)
                logger.debug(f"Added trade {trade.trade_id} to {granularity} bucket")

        for granularity in self.trade_buckets.keys():
            self.trade_buckets[granularity].sort(key=lambda x: x.timestamp, reverse=True)
            logger.info(
                f"Bucket {granularity} now has {len(self.trade_buckets[granularity])} trades"
            )

    def get_completed_buckets(self, current_time: datetime) -> Dict[str, List[List[Trade]]]:
        """Get all completed buckets up to current_time"""

        logger.info(f"Getting completed buckets up to {current_time}")

        completed_buckets: Dict[str, List[List[Trade]]] = {
            "15m": [],
            "1h": [],
            "1d": [],
        }
        if all(len(trades) == 0 for trades in self.trade_buckets.values()):
            logger.info("  No trades in any buckets, returning empty completed buckets")
            return completed_buckets

        for granularity, interval in self.intervals.items():
            if len(self.trade_buckets[granularity]) == 0:
                logger.info(f"  No trades in {granularity} bucket, skipping")
                continue
            logger.info(f"  Processing {granularity} bucket:")
            logger.info(f"    Current trades in bucket: {len(self.trade_buckets[granularity])}")
            # for t in self.trade_buckets[granularity]:
            #     logger.info(f"      {t.instrument} at {t.timestamp}")

            last_time = self.last_bucket_time[granularity]
            logger.info(f"    Last bucket time: {last_time}")

            # Initialize last_time if None
            if last_time is None and self.trade_buckets[granularity]:
                first_trade_time = self.trade_buckets[granularity][-1].timestamp
                last_trade_time = self.trade_buckets[granularity][0].timestamp
                self.last_bucket_time[granularity] = TradeBucketManager.round_time_down(
                    first_trade_time, interval
                )
                logger.info(
                    f"    Initialized {granularity} last bucket time to {self.last_bucket_time[granularity]} ({first_trade_time} <> {last_trade_time})"
                )
                continue

            while last_time and current_time >= last_time + interval:
                next_interval = last_time + interval
                logger.info(f"    Processing interval {last_time} to {next_interval}")

                bucket_trades = self._get_trades_for_interval(
                    granularity, last_time, next_interval
                )

                logger.info(f"      Found {len(bucket_trades)} trades in interval")
                if bucket_trades:
                    completed_buckets[granularity].append(bucket_trades)
                    logger.info(
                        f"      Added bucket with {len(bucket_trades)} trades to {granularity} completed buckets"
                    )

                logger.info(f"      Setting {granularity} last bucket time to {next_interval}")
                self.last_bucket_time[granularity] = next_interval
                last_time = next_interval

            logger.info(
                f"    Remaining trades in {granularity} bucket: {len(self.trade_buckets[granularity])}"
            )

        return completed_buckets

    def _get_trades_for_interval(
        self, granularity: str, start_time: datetime, end_time: datetime
    ) -> List[Trade]:
        """Get trades for a specific time interval and remove them from queue"""
        trades = []
        remaining = []

        logger.debug(f"\nProcessing {granularity} interval {start_time} to {end_time}")
        logger.debug(f"Starting with {len(self.trade_buckets[granularity])} trades")

        for trade in self.trade_buckets[granularity]:
            if start_time <= trade.timestamp < end_time:
                trades.append(trade)
                logger.debug(f"Trade {trade.trade_id} at {trade.timestamp} added to interval")
            elif trade.timestamp >= end_time:
                remaining.append(trade)
                logger.debug(f"Trade {trade.trade_id} at {trade.timestamp} kept for future")

        self.trade_buckets[granularity] = remaining
        logger.debug(
            f"Interval processing complete. Found {len(trades)} trades, {len(remaining)} remaining"
        )

        return trades

    @staticmethod
    def round_time_down(dt: datetime, interval: timedelta) -> datetime:
        """Round datetime down to nearest interval"""
        seconds = int(interval.total_seconds())
        timestamp = int(dt.timestamp())
        return dt - timedelta(seconds=timestamp % seconds)
