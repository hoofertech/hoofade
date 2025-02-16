import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple, override

from config import default_timezone

from .base import TradeSource

logger = logging.getLogger(__name__)


class JsonSource(TradeSource):
    def __init__(
        self,
        source_id: str,
        data_dir: str = "data/flex_reports",
    ):
        super().__init__(source_id)
        self.data_dir = Path(data_dir)
        self.trades_iter = 0
        self.positions_iter = 0
        self.json_done = False

    @override
    async def load_latest_positions_data(self) -> Tuple[dict[str, Any] | None, datetime | None]:
        """Load the most recent portfolio file"""
        data_dir = self.data_dir
        iter = self.positions_iter
        data = JsonSource.load_latest_file(data_dir, "portfolio_*.json", iter)
        if not data:
            return None, None

        flex_stmt = data.get("FlexStatement")
        report_time = None
        if flex_stmt:
            report_time_str = flex_stmt[0].get("whenGenerated")
            if report_time_str:
                report_time = datetime.strptime(report_time_str, "%Y%m%d;%H%M%S").replace(
                    tzinfo=default_timezone()
                )

        self.positions_iter += 1
        return data, report_time

    @staticmethod
    def load_latest_file(data_dir: Path, pattern: str, iter: int) -> Optional[dict[str, Any]]:
        """Load the most recent file matching the pattern from directory"""
        files = sorted(data_dir.glob(pattern))
        if not files:
            return None

        if iter >= len(files):
            return None

        latest_file = files[iter]
        try:
            with open(latest_file) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading file {latest_file}: {e}")
            return None

    @override
    async def load_latest_trades_data(self) -> Tuple[list[dict[str, Any]] | None, datetime | None]:
        """Load the most recent trades file"""
        data_dir = self.data_dir
        iter = self.trades_iter
        data = JsonSource.load_latest_file(data_dir, "trades_*.json", iter)
        self.trades_iter += 1
        if not data:
            self.json_done = True
            return None, None

        stmts = data.get("FlexStatement", [])
        report_time = None
        if stmts:
            report_time_str = stmts[0].get("whenGenerated")
            if report_time_str:
                report_time = datetime.strptime(report_time_str, "%Y%m%d;%H%M%S").replace(
                    tzinfo=default_timezone()
                )
        return data.get("TradeConfirm", []), report_time

    @override
    def is_done(self) -> bool:
        return self.json_done

    @override
    def get_sleep_time(self) -> int:
        return 0
