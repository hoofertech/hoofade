import logging
from datetime import datetime, timezone
import tweepy
from models.message import Message
from sinks.base import MessageSink

logger = logging.getLogger(__name__)


class TwitterSink(MessageSink):
    def __init__(
        self,
        sink_id: str,
        bearer_token: str,
        api_key: str,
        api_secret: str,
        access_token: str,
        access_token_secret: str,
    ):
        super().__init__(sink_id)
        self.client = tweepy.Client(
            bearer_token=bearer_token,
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )
        self.last_publish = datetime.fromtimestamp(0, tz=timezone.utc)

    def can_publish(self) -> bool:
        now = datetime.now(timezone.utc)
        return (now - self.last_publish).total_seconds() >= 60

    async def publish(self, message: Message) -> bool:
        if not self.can_publish():
            return False

        try:
            self.client.create_tweet(text=message.content)
            self.last_publish = datetime.now(timezone.utc)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to Twitter: {str(e)}")
            return False
