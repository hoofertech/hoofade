from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import json
import logging
import pandas as pd
from datetime import datetime
from decimal import Decimal
from models.instrument import Instrument, OptionType
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ParsedPosition:
    instrument: Instrument
    quantity: Decimal
    cost_basis: Decimal
    market_price: Decimal
    currency: str


@dataclass
class ParsedExecution:
    instrument: Instrument
    quantity: Decimal
    price: Decimal
    side: str
    timestamp: datetime
    exec_id: str
    currency: str


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
    def load_latest_trades(data_dir: Path) -> Optional[List[Dict[str, Any]]]:
        """Load the most recent trades file"""
        data = FlexReportParser.load_latest_file(data_dir, "trades_*.json")
        if not data:
            return None
        return data.get("TradeConfirm", [])

    @staticmethod
    def load_latest_portfolio(data_dir: Path) -> Optional[List[Dict[str, Any]]]:
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

    @staticmethod
    def _create_instrument(data: Dict[str, Any]) -> Optional[Instrument]:
        """Create instrument from position or trade data"""
        try:
            put_call = data.get("putCall", "")
            if put_call:
                return Instrument.option(
                    symbol=str(data.get("underlyingSymbol", "")),
                    strike=Decimal(str(data.get("strike", "0"))),
                    expiry=datetime.strptime(str(data["expiry"]), "%Y%m%d").date()
                    if data.get("expiry", "")
                    else None,
                    option_type=OptionType.CALL if put_call == "C" else OptionType.PUT,
                    currency=str(data.get("currency", "USD")),
                )
            else:
                return Instrument.stock(
                    symbol=str(data.get("symbol", "")),
                    currency=str(data.get("currency", "USD")),
                )
        except Exception as e:
            logger.error(f"Error creating instrument: {e}")
            return None

    @staticmethod
    def _row_to_dict(row: Union[pd.Series, Dict[str, Any]]) -> Dict[str, Any]:
        """Convert DataFrame row or dict to consistent format"""
        if isinstance(row, pd.Series):
            return row.to_dict()
        return row

    @staticmethod
    def parse_positions(
        data: Union[pd.DataFrame, List[Dict[str, Any]]] | None,
    ) -> List[ParsedPosition]:
        """Parse positions from DataFrame or list of dicts"""
        if data is None:
            return []

        if isinstance(data, pd.DataFrame):
            if data.empty:
                return []
            data_list = data.to_dict("records")
        else:
            data_list = data

        positions = []
        for item in data_list:
            try:
                item_dict = FlexReportParser._row_to_dict(item)
                instrument = FlexReportParser._create_instrument(item_dict)
                if not instrument:
                    continue

                positions.append(
                    ParsedPosition(
                        instrument=instrument,
                        quantity=Decimal(str(item_dict.get("position", "0"))),
                        cost_basis=Decimal(str(item_dict.get("costBasisPrice", "0"))),
                        market_price=Decimal(str(item_dict.get("markPrice", "0"))),
                        currency=str(item_dict.get("currency", "USD")),
                    )
                )
            except Exception as e:
                logger.error(f"Error parsing position: {e}")
                continue

        return positions

    @staticmethod
    def parse_executions(
        data: Union[pd.DataFrame, List[Dict[str, Any]]] | None,
    ) -> List[ParsedExecution]:
        """Parse executions from DataFrame or list of dicts"""
        if data is None:
            return []

        if isinstance(data, pd.DataFrame):
            if data.empty:
                return []
            data_list = data.to_dict("records")
        else:
            data_list = data

        executions = []
        for item in data_list:
            item_dict = None
            try:
                item_dict = FlexReportParser._row_to_dict(item)
                trade_time = FlexReportParser.parse_flex_datetime(
                    str(item_dict["dateTime"])
                )
                if not trade_time:
                    logger.warning(
                        f"Invalid datetime for trade: {item_dict.get('tradeID', 'unknown')}"
                    )
                    continue

                instrument = FlexReportParser._create_instrument(item_dict)
                if not instrument:
                    continue

                quantity = float(item_dict["quantity"])
                executions.append(
                    ParsedExecution(
                        instrument=instrument,
                        quantity=Decimal(str(abs(quantity))),
                        price=Decimal(str(item_dict["price"])),
                        side="BUY" if quantity > 0 else "SELL",
                        timestamp=trade_time,
                        exec_id=str(item_dict["tradeID"]),
                        currency=str(item_dict["currency"]),
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Error processing trade {item_dict.get('tradeID', 'unknown') if item_dict else 'unknown'}: {e}"
                )
                continue

        return executions

    # Maintain backwards compatibility with existing method names
    parse_positions_from_df = parse_positions
    parse_positions_from_dict = parse_positions
    parse_executions_from_df = parse_executions
    parse_executions_from_dict = parse_executions
