import uuid
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from .base import MessageSink
from models.message import Message
from models.db_message import DBMessage
import logging

logger = logging.getLogger(__name__)


class DatabaseSink(MessageSink):
    def __init__(self, sink_id: str, async_session: async_sessionmaker[AsyncSession]):
        super().__init__(sink_id)
        self.async_session = async_session

    def can_publish(self) -> bool:
        return True

    async def publish(self, message: Message) -> bool:
        async with self.async_session() as session:
            try:
                db_message = DBMessage(
                    id=str(uuid.uuid4()),
                    content=message.content,
                    timestamp=message.timestamp,
                    message_metadata=message.metadata,
                    source_id=self.sink_id,
                    message_type=message.metadata.get("type", "unknown"),
                )

                session.add(db_message)
                await session.commit()
                return True
            except Exception as e:
                logger.error(f"Error in database sink: {str(e)}")
                await session.rollback()
                return False
