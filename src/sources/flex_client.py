import json
import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple

from ib_insync import FlexReport

from config import default_timezone

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

    def _save_report(
        self, report: FlexReport, query_type: str
    ) -> Tuple[datetime | None, dict[str, Any] | None]:
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

        save_dir = self.config.save_dir
        if not save_dir:
            save_dir = tempfile.gettempdir()

        out_save_dir = Path(save_dir)
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
        return when_generated, data

    async def load_report(self, json_path: str) -> Tuple[dict[str, Any] | None, datetime | None]:
        """Load a report from the save directory"""
        with open(json_path, "r") as f:
            return json.load(f)

    async def download_positions(self) -> Tuple[dict[str, Any] | None, datetime | None]:
        """Get current positions"""
        return await self._download_report(
            self.config.portfolio.token, self.config.portfolio.query_id, "portfolio"
        )

    async def download_trades(self) -> Tuple[dict[str, Any] | None, datetime | None]:
        """Get trade executions"""
        return await self._download_report(
            self.config.trades.token, self.config.trades.query_id, "trades"
        )

    async def _download_report(
        self, token: str, query_id: str, report_type: str
    ) -> Tuple[dict[str, Any] | None, datetime | None]:
        """Common method to download and process reports"""
        try:
            report = FlexReport(token=token, queryId=query_id)
            report.download(token, query_id)

            if not report.topics():
                logger.error(f"No data received from IBKR Flex API for {report_type}")
                return None, None

            when_generated, json_data = self._save_report(report, report_type)
            return (json_data, when_generated)
        except Exception as e:
            logger.error(f"Error fetching {report_type}: {str(e)}")
            return (None, None)
