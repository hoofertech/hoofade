import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from ib_insync import FlexReport

from config import default_timezone
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

    def _save_report(self, report: FlexReport, query_type: str) -> datetime | None:
        """Save the raw XML and parsed DataFrame to files"""
        when_generated_str = None
        when_generated = None
        logger.info(f"Report: {report}")
        flex_statement = report.df("FlexStatement")
        if flex_statement is not None:
            flex_statement_recs = flex_statement.to_dict("records")
            if flex_statement_recs:
                when_generated_str = (
                    flex_statement_recs[0].get("whenGenerated")
                    if flex_statement_recs[0] is not None
                    else None
                )
        if when_generated_str:
            when_generated = datetime.strptime(str(when_generated_str), "%Y%m%d;%H%M%S").replace(
                tzinfo=default_timezone()
            )

        if not self.config.save_dir:
            return when_generated

        out_save_dir = Path(self.config.save_dir)
        out_save_dir.mkdir(parents=True, exist_ok=True)
        timestamp = (
            when_generated_str.replace(";", "_")
            if when_generated_str
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
        return when_generated

    async def download_positions(self) -> Tuple[List[Position], datetime | None]:
        """Get current positions"""
        try:
            report = FlexReport(
                token=self.config.portfolio.token,
                queryId=self.config.portfolio.query_id,
            )
            report.download(self.config.portfolio.token, self.config.portfolio.query_id)

            if not report.data:
                logger.error("No data received from IBKR Flex API")
                return ([], None)

            when_generated = self._save_report(report, "portfolio")
            positions = self.parser.parse_positions(report, when_generated)

            return (positions, when_generated)
        except Exception as e:
            logger.error(f"Error fetching positions: {str(e)}")
            return ([], None)

    async def download_trades(self, source_id: str) -> Tuple[List[Trade], datetime | None]:
        """Get trade executions"""
        try:
            report = FlexReport(
                token=self.config.trades.token,
                queryId=self.config.trades.query_id,
            )
            report.download(self.config.trades.token, self.config.trades.query_id)

            if not report.data:
                logger.error("No data received from IBKR Flex API")
                return [], None

            when_generated = self._save_report(report, "trades")
            trades = self.parser.parse_executions(report.df("TradeConfirm"), source_id)

            return trades, when_generated
        except Exception as e:
            logger.error(f"Error fetching trades: {str(e)}")
            return [], None
