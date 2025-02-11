from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple

from models.position import Position
from models.trade import Trade


class TradeSource(ABC):
    def __init__(self, source_id: str):
        self.source_id = source_id

    @abstractmethod
    async def load_positions(self) -> Tuple[bool, datetime | None]:
        """Establish connection to the source"""
        pass

    @abstractmethod
    def get_positions(self) -> List[Position]:
        """Get positions from the source"""
        pass

    @abstractmethod
    async def load_last_day_trades(self) -> Tuple[bool, datetime | None]:
        """Load trades for the last day"""
        pass

    @abstractmethod
    def get_last_day_trades(self) -> List[Trade]:
        """Get trades for the last day"""
        pass

    @abstractmethod
    def is_done(self) -> bool:
        """Check if the source is done"""
        pass

    @abstractmethod
    def get_sleep_time(self) -> int:
        """Get the sleep time for the source"""
        pass
