import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Tuple

from models.position import Position
from models.trade import Trade
from sources.ibkr_parser import FlexReportParser

logger = logging.getLogger(__name__)


class TradeSource(ABC):
    def __init__(self, source_id: str):
        self.source_id = source_id
        self.positions: List[Position] = []
        self.last_day_trades: List[Trade] = []
        self.parser = FlexReportParser()

    async def load_positions(self) -> Tuple[bool, datetime | None]:
        try:
            positions_data, when_generated = await self.load_latest_positions_data()
            if not positions_data:
                logger.warning(f"No positions data found for {self.source_id}")
                return when_generated is not None, when_generated

            self.positions = FlexReportParser.parse_positions(positions_data, when_generated)
            return when_generated is not None, when_generated
        except Exception as e:
            logger.error(f"Error connecting to JSON source: {e}")
            return False, None

    @abstractmethod
    async def load_latest_positions_data(self) -> Tuple[dict[str, Any] | None, datetime | None]:
        """Load the most recent portfolio file"""
        pass

    def get_positions(self) -> list[Position]:
        """Get positions from the source"""
        return self.positions

    async def load_last_day_trades(self) -> Tuple[bool, datetime | None]:
        try:
            trades_data, _when_generated = await self.load_latest_trades_data()

            if not trades_data:
                logger.warning(f"No trades data found for {self.source_id}")
                return (True, None)

            parsed_trades = self.parser.parse_executions(trades_data, self.source_id)
            if not parsed_trades:
                logger.warning(f"No parsed trades data found for {self.source_id}")
                return (True, None)

            since = TradeSource.get_min_datetime_for_last_day(parsed_trades)
            self.last_day_trades = [trade for trade in parsed_trades if trade.timestamp >= since]
            logger.debug(f"Loaded {len(self.last_day_trades)} trades for {self.source_id}")

            # Get the latest timestamp from the trades
            latest_timestamp = (
                max(trade.timestamp for trade in self.last_day_trades)
                if self.last_day_trades
                else None
            )
            return (True, latest_timestamp)

        except Exception as e:
            logger.error(f"Error fetching trades: {e}", exc_info=True)
            return (False, None)

    @abstractmethod
    async def load_latest_trades_data(self) -> Tuple[list[dict[str, Any]] | None, datetime | None]:
        """Load the most recent trades file"""
        pass

    @staticmethod
    def get_min_datetime_for_last_day(trades: list[Trade]) -> datetime:
        last_day_in_data = max(trade.timestamp for trade in trades)
        return last_day_in_data.replace(hour=0, minute=0, second=0, microsecond=0)

    def get_last_day_trades(self) -> list[Trade]:
        """Get trades for the last day"""
        return self.last_day_trades

    @abstractmethod
    def is_done(self) -> bool:
        """Check if the source is done"""
        pass

    @abstractmethod
    def get_sleep_time(self) -> int:
        """Get the sleep time for the source"""
        pass
