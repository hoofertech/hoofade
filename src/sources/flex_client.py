from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List
import logging
import pandas as pd
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
    symbol: str
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
        df = report.df("TradeConfirm")  # Using TradeConfirm as shown in the notebook
        if df is None or df.empty:
            return []

        executions = []
        for _, row in df.iterrows():
            # Convert datetime string to datetime object with timezone
            trade_datetime = pd.to_datetime(str(row["dateTime"]), utc=True)
            if pd.isna(trade_datetime):
                logger.warning(f"Invalid datetime for trade: {row['tradeID']}")
                continue

            executions.append(
                Execution(
                    symbol=str(row["symbol"]),
                    quantity=Decimal(str(abs(row["quantity"]))),
                    price=Decimal(str(row["price"])),
                    side="BUY" if float(row["quantity"]) > 0 else "SELL",
                    timestamp=trade_datetime,
                    exec_id=str(
                        row["tradeID"]
                    ),  # Using tradeID as shown in the notebook
                )
            )
        return executions

    def _check_report_topics(self, report: FlexReport) -> None:
        """Debug helper to check available topics in the report"""
        topics = report.topics()
        logger.info(f"Available topics in report: {topics}")
