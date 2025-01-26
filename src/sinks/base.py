from abc import ABC, abstractmethod
from src.models.message import Message


class MessageSink(ABC):
    def __init__(self, sink_id: str):
        self.sink_id = sink_id

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the sink"""
        pass

    @abstractmethod
    def publish(self, message: Message) -> bool:
        """Publish a message to the sink"""
        pass

    @abstractmethod
    def can_publish(self) -> bool:
        """Check if sink can accept new messages (e.g., rate limits)"""
        pass
