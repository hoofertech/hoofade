from abc import ABC, abstractmethod
from typing import AsyncIterator
from models.trade import Trade
from models.position import Position


class TradeSource(ABC):
    def __init__(self, source_id: str):
        self.source_id = source_id

    @abstractmethod
    async def load_positions(self) -> bool:
        """Establish connection to the source"""
        pass

    @abstractmethod
    def get_positions(self) -> AsyncIterator[Position]:
        """Get positions from the source"""
        pass

    @abstractmethod
    def get_last_day_trades(self) -> AsyncIterator[Trade]:
        """Get trades for the last day"""
        pass
