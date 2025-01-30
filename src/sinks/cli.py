import logging
from models.message import Message
from sinks.base import MessageSink

logger = logging.getLogger(__name__)


class CLISink(MessageSink):
    def __init__(self, sink_id: str):
        super().__init__(sink_id)

    def can_publish(self) -> bool:
        # CLI sink can always publish
        return True

    async def publish(self, message: Message) -> bool:
        try:
            print(
                f"[{message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {message.content}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to publish to CLI: {str(e)}")
            return False
