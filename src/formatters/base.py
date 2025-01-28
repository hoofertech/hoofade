from abc import ABC, abstractmethod
from models.trade import Trade
from models.message import Message
from typing import Optional


class MessageFormatter(ABC):
    @abstractmethod
    def format_trade(
        self, trade: Trade, matching_trade: Optional[Trade] = None
    ) -> Message:
        """Format a trade into a message"""
        pass
