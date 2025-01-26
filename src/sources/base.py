from abc import ABC, abstractmethod
from typing import Iterator
from datetime import datetime
from src.models.trade import Trade


class TradeSource(ABC):
    def __init__(self, source_id: str):
        self.source_id = source_id

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the source"""
        pass

    @abstractmethod
    def get_recent_trades(self, since: datetime) -> Iterator[Trade]:
        """Get trades since the specified timestamp"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Clean up any connections"""
        pass
