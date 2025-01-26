from abc import ABC, abstractmethod
from src.models.trade import Trade
from src.models.message import Message
from typing import Optional


class MessageFormatter(ABC):
    @abstractmethod
    def format_trade(
        self, trade: Trade, matching_trade: Optional[Trade] = None
    ) -> Message:
        """Format a trade into a message"""
        pass
