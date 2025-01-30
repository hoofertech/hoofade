from datetime import datetime
from typing import AsyncIterator, List
from pathlib import Path
import logging
from models.trade import Trade
from models.position import Position
from .base import TradeSource
from .ibkr_parser import FlexReportParser
from .ibkr_parser import ParsedExecution
logger = logging.getLogger(__name__)


class JsonSource(TradeSource):
    def __init__(
        self,
        source_id: str,
        data_dir: str = "data/flex_reports",
    ):
        super().__init__(source_id)
        self.data_dir = Path(data_dir)
        self.positions: List[Position] = []
        self.parser = FlexReportParser()

    async def connect(self) -> bool:
        try:
            positions_data = self.parser.load_latest_portfolio(self.data_dir)
            if positions_data is None:
                return False

            parsed_positions = self.parser.parse_positions_from_dict(positions_data)
            self.positions = [
                Position(
                    instrument=pos.instrument,
                    quantity=pos.quantity,
                    cost_basis=pos.cost_basis,
                    market_price=pos.market_price,
                )
                for pos in parsed_positions
            ]
            return True
        except Exception as e:
            logger.error(f"Error connecting to JSON source: {e}")
            return False

    async def disconnect(self) -> None:
        """Clean up any connections"""
        self.positions = []

    async def get_positions(self) -> List[Position]:
        """Get current positions"""
        return self.positions

    async def get_last_day_trades(self) -> AsyncIterator[Trade]:
        try:
            trades_data = self.parser.load_latest_trades(self.data_dir)
            if trades_data is None:
                return

            parsed_executions = self.parser.parse_executions_from_dict(trades_data)
            if not parsed_executions:
                return
            since = self.get_min_datetime_for_last_day(parsed_executions)
            for exec in parsed_executions:
                if exec.timestamp >= since:
                    yield Trade(
                        instrument=exec.instrument,
                        quantity=exec.quantity,
                        price=exec.price,
                        currency=exec.currency,
                        side=exec.side,
                        timestamp=exec.timestamp,
                        source_id=self.source_id,
                        trade_id=exec.exec_id,
                    )
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")

    @staticmethod
    def get_min_datetime_for_last_day(executions: List[ParsedExecution]) -> datetime:
        last_day_in_data = max(exec.timestamp for exec in executions)
        return last_day_in_data.replace(hour=0, minute=0, second=0, microsecond=0)
