import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from config import default_timezone
from models.position import Position
from models.trade import Trade

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

    async def load_positions(self) -> Tuple[bool, datetime | None]:
        try:
            positions_data, when_generated = JsonSource.load_latest_portfolio(
                self.data_dir, self.positions_iter
            )
            self.positions_iter += 1
            if not positions_data:
                logger.error(f"No positions data found for {self.source_id}")
                return False, None

            self.positions = positions_data
            return when_generated is not None, when_generated
        except Exception as e:
            logger.error(f"Error connecting to JSON source: {e}")
            return False, None

    @staticmethod
    def load_latest_portfolio(data_dir: Path, iter: int) -> Tuple[List[Position], datetime | None]:
        """Load the most recent portfolio file"""
        data = FlexReportParser.load_latest_file(data_dir, "portfolio_*.json", iter)
        if not data:
            return [], None

        report_time_str = data.get("whenGenerated")
        report_time = None
        if report_time_str:
            report_time = datetime.strptime(report_time_str, "%Y%m%d;%H%M%S").replace(
                tzinfo=default_timezone()
            )

        return FlexReportParser.parse_positions(data, report_time), report_time

    def get_positions(self) -> List[Position]:
        """Get current positions"""
        return self.positions

    async def load_last_day_trades(self) -> Tuple[bool, datetime | None]:
        try:
            trades_data = JsonSource.load_latest_trades(self.data_dir, self.trades_iter)
            self.trades_iter += 1

            if trades_data is None:
                self.json_done = True
                return (True, None)

            parsed_trades = self.parser.parse_executions_from_dict(trades_data, self.source_id)
            if not parsed_trades:
                return (True, None)

            since = self.get_min_datetime_for_last_day(parsed_trades)
            self.last_day_trades = [trade for trade in parsed_trades if trade.timestamp >= since]
            logger.info(f"Loaded {len(self.last_day_trades)} trades for {self.source_id}")

            # Get the latest timestamp from the trades
            latest_timestamp = (
                max(trade.timestamp for trade in self.last_day_trades)
                if self.last_day_trades
                else None
            )
            return (True, latest_timestamp)

        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return (False, None)

    @staticmethod
    def load_latest_trades(data_dir: Path, iter: int) -> List[Dict[str, Any]] | None:
        """Load the most recent trades file"""
        data = FlexReportParser.load_latest_file(data_dir, "trades_*.json", iter)
        if not data:
            return None
        return data.get("TradeConfirm", [])

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
