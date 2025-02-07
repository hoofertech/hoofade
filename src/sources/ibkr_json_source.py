from datetime import datetime
from typing import List
from pathlib import Path
import logging
from models.trade import Trade
from models.position import Position
from .base import TradeSource
from .ibkr_parser import FlexReportParser

logger = logging.getLogger(__name__)


class JsonSource(TradeSource):
    def __init__(
        self,
        source_id: str,
        data_dir: str = "data/flex_reports",
    ):
        super().__init__(source_id)
        self.data_dir = Path(data_dir)
        self.positions: List[Position] = []
        self.parser = FlexReportParser()
        self.trades_iter = 0
        self.positions_iter = 0
        self.last_day_trades: List[Trade] = []
        self.json_done = False

    async def load_positions(self) -> bool:
        try:
            positions_data = self.parser.load_latest_portfolio(
                self.data_dir, self.positions_iter
            )
            self.positions_iter += 1
            if positions_data is None:
                logger.error(f"No positions data found for {self.source_id}")
                return False

            self.positions = positions_data
            return True
        except Exception as e:
            logger.error(f"Error connecting to JSON source: {e}")
            return False

    def get_positions(self) -> List[Position]:
        """Get current positions"""
        return self.positions

    async def load_last_day_trades(self) -> bool:
        self.last_day_trades = []
        try:
            trades_data = self.parser.load_latest_trades(
                self.data_dir, self.trades_iter
            )
            self.trades_iter += 1
            if trades_data is None:
                self.json_done = True
                return True

            parsed_trades = self.parser.parse_executions_from_dict(
                trades_data, self.source_id
            )
            if not parsed_trades:
                return True
            since = self.get_min_datetime_for_last_day(parsed_trades)
            self.last_day_trades = [
                trade for trade in parsed_trades if trade.timestamp >= since
            ]
            logger.info(
                f"Loaded {len(self.last_day_trades)} trades for {self.source_id}"
            )
            return True
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return False

    def get_last_day_trades(self) -> List[Trade]:
        return self.last_day_trades

    @staticmethod
    def get_min_datetime_for_last_day(trades: List[Trade]) -> datetime:
        last_day_in_data = max(trade.timestamp for trade in trades)
        return last_day_in_data.replace(hour=0, minute=0, second=0, microsecond=0)

    def is_done(self) -> bool:
        return self.json_done

    def get_sleep_time(self) -> int:
        return 1
