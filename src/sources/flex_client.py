from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List
import logging
import pandas as pd
from models.instrument import Instrument, OptionType
from ib_insync import FlexReport
from pathlib import Path
import json

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
        self,
        portfolio_config: FlexQueryConfig,
        trades_config: FlexQueryConfig,
        save_dir: str | None = None,
    ):
        self.portfolio_config = portfolio_config
        self.trades_config = trades_config
        self.save_dir = save_dir

    def _save_report(self, report: FlexReport, query_type: str) -> None:
        """Save the raw XML and parsed DataFrame to files"""
        if not self.save_dir:
            return

        out_save_dir = Path(self.save_dir)
        out_save_dir.mkdir(parents=True, exist_ok=True)

        """Save the raw XML and parsed DataFrame to files"""
        # Extract timestamp from the XML file's whenGenerated attribute
        when_generated = None
        flex_statement = report.root.find(".//FlexStatement")

        if flex_statement is not None:
            when_generated = flex_statement.get("whenGenerated")

        if when_generated:
            # Convert YYYYMMDD;HHMMSS to YYYYMMDD_HHMMSS
            timestamp = when_generated.replace(";", "_")
        else:
            # Fallback to current time if whenGenerated is not found
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            logger.warning("Could not find whenGenerated in XML, using current time")

        # Save XML file
        xml_path = out_save_dir / f"{query_type}_{timestamp}.xml"
        logger.info(f"saving xml to {xml_path}")
        report.save(str(xml_path))

        # Save parsed DataFrames
        data = {}
        for topic in report.topics():
            df = report.df(topic)
            if df is not None and not df.empty:
                data[topic] = df.to_dict(orient="records")

        json_path = out_save_dir / f"{query_type}_{timestamp}.json"
        logger.info(f"saving json to {json_path}")
        with open(json_path, "w") as f:
            json.dump(data, f, default=str)

        logger.info(f"Saved {query_type} report to {xml_path} and {json_path}")

    def _download_flex_report(self, token: str, query_id: str) -> FlexReport:
        try:
            logger.info(f"Downloading Flex report: {token} {query_id}")
            report = FlexReport(token=token, queryId=query_id)
            report.download(token, query_id)

            logger.info(f"token: {token}")
            logger.info(f"portfolio_config.token: {self.portfolio_config.token}")
            logger.info(f"trades_config.token: {self.trades_config.token}")
            print(f"report: {report}")

            if not report.data:
                logger.error("No data received from IBKR Flex API")
                raise Exception("No data received from IBKR Flex API")

            logger.info(f"Available topics in report: {report.topics()}")

            # Save the report
            query_type = (
                "portfolio" if query_id == self.portfolio_config.query_id else "trades"
            )
            self._save_report(report, query_type)

            return report
        except Exception as e:
            logger.error(f"Failed to download Flex report: {str(e)}")
            raise

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
