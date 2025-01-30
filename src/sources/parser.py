from pathlib import Path
import json
from typing import Dict, Any, Optional
import logging
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class FlexReportParser:
    @staticmethod
    def load_latest_file(data_dir: Path, pattern: str) -> Optional[Dict[str, Any]]:
        """Load the most recent file matching the pattern from directory"""
        files = sorted(data_dir.glob(pattern))
        if not files:
            return None

        latest_file = files[-1]
        try:
            with open(latest_file) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading file {latest_file}: {e}")
            return None

    @staticmethod
    def load_latest_trades(data_dir: Path) -> Optional[Dict[str, Any]]:
        """Load the most recent trades file"""
        data = FlexReportParser.load_latest_file(data_dir, "trades_*.json")
        if not data:
            return None
        return data.get("TradeConfirm", [])

    @staticmethod
    def load_latest_portfolio(data_dir: Path) -> Optional[Dict[str, Any]]:
        """Load the most recent portfolio file"""
        data = FlexReportParser.load_latest_file(data_dir, "portfolio_*.json")
        if not data:
            return None
        return data.get("OpenPosition", [])

    @staticmethod
    def parse_flex_datetime(datetime_str: str) -> Optional[datetime]:
        """Parse datetime from IBKR Flex format"""
        try:
            if ";" in datetime_str:
                # Handle IBKR Flex Query format: "20250129;112309"
                return pd.to_datetime(
                    datetime_str.replace(";", " "), format="%Y%m%d %H%M%S", utc=True
                ).to_pydatetime()
            else:
                # Handle other possible formats (like ISO format)
                dt = pd.to_datetime(datetime_str, utc=True)
                return dt.to_pydatetime()
        except Exception as e:
            logger.error(f"Error parsing datetime {datetime_str}: {e}")
            return None
