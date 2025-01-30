from pathlib import Path
from typing import Dict, Any, Optional, List
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


@dataclass
class ParsedExecution:
    instrument: Instrument
    quantity: Decimal
    price: Decimal
    side: str
    timestamp: datetime
    exec_id: str


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
    def parse_positions_from_df(df: pd.DataFrame | None) -> List[ParsedPosition]:
        """Parse positions from a DataFrame"""
        if df is None or len(df.index) == 0:
            return []

        positions = []
        for _, row in df.iterrows():
            try:
                put_call = row.get("putCall", "")
                if put_call:
                    instrument = Instrument.option(
                        symbol=str(row.get("underlyingSymbol", "")),
                        strike=Decimal(str(row.get("strike", "0"))),
                        expiry=datetime.strptime(str(row["expiry"]), "%Y%m%d").date()
                        if row.get("expiry", "")
                        else None,
                        option_type=OptionType.CALL
                        if put_call == "C"
                        else OptionType.PUT,
                    )
                else:
                    instrument = Instrument.stock(symbol=str(row.get("symbol", "")))

                positions.append(
                    ParsedPosition(
                        instrument=instrument,
                        quantity=Decimal(str(row.get("position", "0"))),
                        cost_basis=Decimal(str(row.get("costBasisPrice", "0"))),
                        market_price=Decimal(str(row.get("markPrice", "0"))),
                    )
                )
            except Exception as e:
                logger.error(f"Error parsing position: {e}")
                continue

        return positions

    @staticmethod
    def parse_positions_from_dict(
        positions_data: List[Dict[str, Any]],
    ) -> List[ParsedPosition]:
        """Parse positions from a list of dictionaries"""
        positions = []
        for pos_data in positions_data:
            try:
                put_call = pos_data.get("putCall", "")
                if put_call:
                    instrument = Instrument.option(
                        symbol=str(pos_data.get("underlyingSymbol", "")),
                        strike=Decimal(str(pos_data.get("strike", "0"))),
                        expiry=datetime.strptime(
                            str(pos_data["expiry"]), "%Y%m%d"
                        ).date()
                        if pos_data.get("expiry", "")
                        else None,
                        option_type=OptionType.CALL
                        if put_call == "C"
                        else OptionType.PUT,
                    )
                else:
                    instrument = Instrument.stock(
                        symbol=str(pos_data.get("symbol", ""))
                    )

                positions.append(
                    ParsedPosition(
                        instrument=instrument,
                        quantity=Decimal(str(pos_data.get("position", "0"))),
                        cost_basis=Decimal(str(pos_data.get("costBasisPrice", "0"))),
                        market_price=Decimal(str(pos_data.get("markPrice", "0"))),
                    )
                )
            except Exception as e:
                logger.error(f"Error parsing position: {e}")
                continue

        return positions

    @staticmethod
    def parse_executions_from_df(df: pd.DataFrame | None) -> List[ParsedExecution]:
        """Parse executions from a DataFrame"""
        if df is None or len(df.index) == 0:
            return []

        executions = []
        for _, row in df.iterrows():
            try:
                trade_time = FlexReportParser.parse_flex_datetime(str(row["dateTime"]))
                if not trade_time:
                    logger.warning(f"Invalid datetime for trade: {row['tradeID']}")
                    continue

                put_call = row.get("putCall", "")
                if put_call:
                    instrument = Instrument.option(
                        symbol=str(row.get("underlyingSymbol", "")),
                        strike=Decimal(str(row.get("strike", "0"))),
                        expiry=datetime.strptime(str(row["expiry"]), "%Y%m%d").date()
                        if row.get("expiry", "")
                        else None,
                        option_type=OptionType.CALL
                        if put_call == "C"
                        else OptionType.PUT,
                    )
                else:
                    instrument = Instrument.stock(symbol=str(row.get("symbol", "")))

                executions.append(
                    ParsedExecution(
                        instrument=instrument,
                        quantity=Decimal(str(abs(float(row["quantity"])))),
                        price=Decimal(str(row["price"])),
                        side="BUY" if float(row["quantity"]) > 0 else "SELL",
                        timestamp=trade_time,
                        exec_id=str(row["tradeID"]),
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Error processing trade from df {row.get('tradeID', 'unknown')}, row: {row}, error: {e}"
                )
                continue

        return executions

    @staticmethod
    def parse_executions_from_dict(
        trades_data: List[Dict[str, Any]],
    ) -> List[ParsedExecution]:
        """Parse executions from a list of dictionaries"""
        executions = []
        for trade_data in trades_data:
            try:
                trade_time = FlexReportParser.parse_flex_datetime(
                    str(trade_data["dateTime"])
                )
                if not trade_time:
                    logger.warning(
                        f"Invalid datetime for trade: {trade_data['tradeID']}"
                    )
                    continue

                put_call = trade_data.get("putCall", "")
                if put_call:
                    instrument = Instrument.option(
                        symbol=str(trade_data.get("underlyingSymbol", "")),
                        strike=Decimal(str(trade_data.get("strike", "0"))),
                        expiry=datetime.strptime(
                            str(trade_data["expiry"]), "%Y%m%d"
                        ).date()
                        if trade_data.get("expiry", "")
                        else None,
                        option_type=OptionType.CALL
                        if put_call == "C"
                        else OptionType.PUT,
                    )
                else:
                    instrument = Instrument.stock(
                        symbol=str(trade_data.get("symbol", ""))
                    )

                executions.append(
                    ParsedExecution(
                        instrument=instrument,
                        quantity=Decimal(str(abs(float(trade_data["quantity"])))),
                        price=Decimal(str(trade_data["price"])),
                        side="BUY" if float(trade_data["quantity"]) > 0 else "SELL",
                        timestamp=trade_time,
                        exec_id=str(trade_data["tradeID"]),
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Error processing trade from dict {trade_data.get('tradeID', 'unknown')}: {e}"
                )
                continue

        return executions
