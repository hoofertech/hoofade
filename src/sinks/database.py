import logging
import uuid

from database import Database
from models.db_message import DBMessage
from models.message import Message

from .base import MessageSink

logger = logging.getLogger(__name__)


class DatabaseSink(MessageSink):
    def __init__(self, sink_id: str, db: Database):
        super().__init__(sink_id)
        self.db = db

    def can_publish(self) -> bool:
        return True

    async def publish(self, message: Message) -> bool:
        try:
            db_message = DBMessage(
                id=str(uuid.uuid4()),
                content=message.content,
                timestamp=message.timestamp,
                message_metadata=message.metadata,
                source_id=self.sink_id,
                message_type=message.metadata.get("type", "unknown"),
            )

            await self.db.save_message(db_message)
            return True
        except Exception as e:
            logger.error(f"Error in database sink: {str(e)}")
            return False
