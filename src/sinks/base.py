import asyncio

# Set up event loop
try:
    _event_loop = asyncio.get_running_loop()
except RuntimeError:
    _event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_event_loop)

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from models.position import Position
from models.trade import Trade


class MessageSink(ABC):
    PUBLISH_PORTFOLIO_AFTER_EACH_TRADE = False

    def __init__(self, sink_id: str):
        self.sink_id = sink_id

    def can_publish(self, message_type: str | None = None) -> bool:
        return True

    @abstractmethod
    async def publish_trades(self, trades: List[Trade], now: datetime) -> bool:
        """Publish trades to the sink"""
        pass

    @abstractmethod
    async def publish_portfolio(self, positions: List[Position], now: datetime) -> bool:
        """Publish portfolio to the sink"""
        pass

    @abstractmethod
    def update_portfolio(self, positions: List[Position]) -> bool:
        """Update portfolio to the sink"""
        pass
