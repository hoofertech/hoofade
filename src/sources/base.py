from abc import ABC, abstractmethod
from typing import AsyncIterator
from datetime import datetime
from models.trade import Trade


class TradeSource(ABC):
    def __init__(self, source_id: str):
        self.source_id = source_id

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the source"""
        pass

    @abstractmethod
    def get_recent_trades(self, since: datetime) -> AsyncIterator[Trade]:
        """Get trades since the specified timestamp"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up any connections"""
        pass
