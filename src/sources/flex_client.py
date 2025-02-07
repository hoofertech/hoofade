import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

from ib_insync import FlexReport

from models.position import Position
from models.trade import Trade

from .ibkr_parser import FlexReportParser

logger = logging.getLogger(__name__)


@dataclass
class FlexQueryConfig:
    token: str
    query_id: str


@dataclass
class FlexClientConfig:
    portfolio: FlexQueryConfig
    trades: FlexQueryConfig
    save_dir: str | None = None


class FlexClient:
    def __init__(
        self,
        config: FlexClientConfig,
    ):
        self.config = config
        self.parser = FlexReportParser()

    def _save_report(self, report: FlexReport, query_type: str) -> None:
        """Save the raw XML and parsed DataFrame to files"""
        if not self.config.save_dir:
            return

        out_save_dir = Path(self.config.save_dir)
        out_save_dir.mkdir(parents=True, exist_ok=True)
        flex_statement = report.root.find(".//FlexStatement")
        when_generated = (
            flex_statement.get("whenGenerated") if flex_statement is not None else None
        )
        timestamp = (
            when_generated.replace(";", "_")
            if when_generated
            else datetime.now().strftime("%Y%m%d_%H%M%S")
        )

        # Save XML file
        xml_path = out_save_dir / f"{query_type}_{timestamp}.xml"
        report.save(str(xml_path))

        # Save parsed DataFrames as JSON
        data = {}
        for topic in report.topics():
            df = report.df(topic)
            if df is not None and not df.empty:
                data[topic] = df.to_dict(orient="records")

        json_path = out_save_dir / f"{query_type}_{timestamp}.json"
        with open(json_path, "w") as f:
            json.dump(data, f, default=str)

        logger.info(f"Saved {query_type} report to {xml_path} and {json_path}")

    async def get_positions(self) -> AsyncIterator[Position]:
        """Get current positions"""
        try:
            report = FlexReport(
                token=self.config.portfolio.token,
                queryId=self.config.portfolio.query_id,
            )
            report.download(self.config.portfolio.token, self.config.portfolio.query_id)

            if not report.data:
                logger.error("No data received from IBKR Flex API")
                return

            self._save_report(report, "portfolio")

            for pos in self.parser.parse_positions(report):
                yield pos
        except Exception as e:
            logger.error(f"Error fetching positions: {str(e)}")
            return

    async def get_trades(self, source_id: str) -> AsyncIterator[Trade]:
        """Get trade executions"""
        try:
            report = FlexReport(
                token=self.config.trades.token,
                queryId=self.config.trades.query_id,
            )
            report.download(self.config.trades.token, self.config.trades.query_id)
            if not report.data:
                logger.error("No data received from IBKR Flex API")
                return

            self._save_report(report, "trades")
            for exec in self.parser.parse_executions_from_df(report.df("TradeConfirm"), source_id):
                yield exec
        except Exception as e:
            logger.error(f"Error fetching executions: {str(e)}")
            return
