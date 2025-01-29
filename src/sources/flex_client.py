from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List
import logging
import pandas as pd
from models.instrument import Instrument, OptionType
from ib_insync import FlexReport

logger = logging.getLogger(__name__)


@dataclass
class FlexQueryConfig:
    token: str
    query_id: str


@dataclass
class Position:
    symbol: str
    quantity: Decimal
    cost_basis: Decimal
    market_price: Decimal


@dataclass
class Execution:
    instrument: Instrument
    quantity: Decimal
    price: Decimal
    side: str
    timestamp: datetime
    exec_id: str


class FlexClient:
    def __init__(
        self, portfolio_config: FlexQueryConfig, trades_config: FlexQueryConfig
    ):
        self.portfolio_config = portfolio_config
        self.trades_config = trades_config

    async def get_positions(self) -> List[Position]:
        report = self._download_flex_report(
            self.portfolio_config.token, self.portfolio_config.query_id
        )
        return self._parse_positions(report)

    async def get_executions(self) -> List[Execution]:
        report = self._download_flex_report(
            self.trades_config.token, self.trades_config.query_id
        )
        logger.info(f"Report: {report.df('TradeConfirm')}")
        return self._parse_executions(report)

    def _download_flex_report(self, token: str, query_id: str) -> FlexReport:
        """Download flex report synchronously"""
        try:
            logger.info(f"Downloading Flex report: {token} {query_id}")
            report = FlexReport(token=token, queryId=query_id)
            report.download(token, query_id)  # Synchronous call
            return report
        except Exception as e:
            logger.error(f"Failed to download Flex report: {str(e)}")
            raise

    def _parse_positions(self, report: FlexReport) -> List[Position]:
        df = report.df("Position")
        if df is None or df.empty:
            return []

        positions = []
        for _, row in df.iterrows():
            positions.append(
                Position(
                    symbol=str(row["symbol"]),
                    quantity=Decimal(str(row["position"])),
                    cost_basis=Decimal(str(row["costBasis"])),
                    market_price=Decimal(str(row["markPrice"])),
                )
            )
        return positions

    def _parse_executions(self, report: FlexReport) -> List[Execution]:
        df = report.df("TradeConfirm")
        if df is None or len(df.index) == 0:  # Changed from df.empty
            return []

        executions = []
        for _, row in df.iterrows():
            try:
                trade_datetime = pd.to_datetime(str(row["dateTime"]), utc=True)
                if pd.isna(trade_datetime):
                    logger.warning(f"Invalid datetime for trade: {row['tradeID']}")
                    continue

                # Parse instrument details
                symbol = str(row["symbol"])

                # Check if this is an option trade by checking putCall column
                put_call_value = row.get("putCall")
                is_option = isinstance(put_call_value, str) and put_call_value.strip()

                if is_option:  # This is an option
                    try:
                        expiry = pd.to_datetime(str(row["expiry"])).date()
                        strike = Decimal(str(row["strike"]))
                        option_type = (
                            OptionType.CALL
                            if str(put_call_value).upper() == "C"
                            else OptionType.PUT
                        )
                        instrument = Instrument.option(
                            symbol=symbol,
                            strike=strike,
                            expiry=expiry,
                            option_type=option_type,
                        )
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Invalid option data for trade {row['tradeID']}: {e}"
                        )
                        continue
                else:  # This is a stock
                    instrument = Instrument.stock(symbol=symbol)

                executions.append(
                    Execution(
                        instrument=instrument,
                        quantity=Decimal(str(abs(row["quantity"]))),
                        price=Decimal(str(row["price"])),
                        side="BUY" if float(row["quantity"]) > 0 else "SELL",
                        timestamp=trade_datetime,
                        exec_id=str(row["tradeID"]),
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Error processing trade {row.get('tradeID', 'unknown')}: {e}"
                )
                continue

        return executions
