from abc import ABC, abstractmethod
from typing import AsyncIterator
from models.trade import Trade


class TradeSource(ABC):
    def __init__(self, source_id: str):
        self.source_id = source_id

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the source"""
        pass

    @abstractmethod
    def get_last_day_trades(self) -> AsyncIterator[Trade]:
        """Get trades for the last day"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up any connections"""
        pass
