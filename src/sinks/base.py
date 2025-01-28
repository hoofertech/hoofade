from abc import ABC, abstractmethod
from models.message import Message


class MessageSink(ABC):
    def __init__(self, sink_id: str):
        self.sink_id = sink_id

    @abstractmethod
    async def publish(self, message: Message) -> bool:
        """Publish a message to the sink"""
        pass

    @abstractmethod
    def can_publish(self) -> bool:
        """Check if the sink can accept new messages"""
        pass
