from datetime import datetime
from typing import AsyncIterator, List
from pathlib import Path
import logging
from models.trade import Trade
from models.position import Position
from .base import TradeSource
from .ibkr_parser import FlexReportParser

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
        self.iter = 0
        self.last_day_trades: List[Trade] = []
        self.json_done = False

    async def load_positions(self) -> bool:
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

    async def get_positions(self) -> AsyncIterator[Position]:
        """Get current positions"""
        for pos in self.positions:
            yield pos

    async def load_last_day_trades(self) -> bool:
        NUM_TRADERS_PER_ITERATION = 10
        try:
            trades_data = self.parser.load_latest_trades(self.data_dir)
            if trades_data is None:
                return

            parsed_trades = self.parser.parse_executions_from_dict(
                trades_data, self.source_id
            )
            if not parsed_trades:
                return
            since = self.get_min_datetime_for_last_day(parsed_trades)
            self.last_day_trades = [
                trade for trade in parsed_trades if trade.timestamp >= since
            ]
            if self.iter * NUM_TRADERS_PER_ITERATION < len(parsed_trades):
                start_index = self.iter * NUM_TRADERS_PER_ITERATION
                end_index = min(start_index + NUM_TRADERS_PER_ITERATION, len(parsed_trades))
                self.last_day_trades = [
                    trade for trade in parsed_trades[start_index:end_index] if trade.timestamp >= since
                ]
            else:
                self.last_day_trades = []
                self.json_done = True
            logger.info(f"Loaded {len(self.last_day_trades)} trades for {self.source_id}")
            self.iter += 1
            return True
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return False

    def get_last_day_trades(self) -> List[Trade]:
        return self.last_day_trades

    @staticmethod
    def get_min_datetime_for_last_day(trades: List[Trade]) -> datetime:
        last_day_in_data = max(trade.timestamp for trade in trades)
        return last_day_in_data.replace(hour=0, minute=0, second=0, microsecond=0)

    def is_done(self) -> bool:
        return self.json_done

    def get_sleep_time(self) -> int:
        return 1
