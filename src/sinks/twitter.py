import tweepy
import logging
from datetime import datetime, timedelta, timezone
from collections import deque
from src.models.message import Message
from src.sinks.base import MessageSink

logger = logging.getLogger(__name__)


class TwitterSink(MessageSink):
    MAX_TWEETS_PER_MONTH = 500
    MAX_TWEETS_PER_DAY = 12

    def __init__(self, sink_id: str, credentials: dict):
        super().__init__(sink_id)
        self.credentials = credentials
        self.client = None
        self.daily_messages = deque(maxlen=self.MAX_TWEETS_PER_DAY)
        self.monthly_messages = deque(maxlen=self.MAX_TWEETS_PER_MONTH)

    def connect(self) -> bool:
        try:
            self.client = tweepy.Client(**self.credentials)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Twitter: {str(e)}")
            return False

    def can_publish(self) -> bool:
        now = datetime.now(timezone.utc)
        self._clean_message_history(now)
        return (
            len(self.daily_messages) < self.MAX_TWEETS_PER_DAY
            and len(self.monthly_messages) < self.MAX_TWEETS_PER_MONTH
        )

    def publish(self, message: Message) -> bool:
        if not self.can_publish() or not self.client:
            return False

        try:
            response = self.client.create_tweet(text=message.content)
            if response and getattr(response, "data", None):
                now = datetime.now(timezone.utc)
                self.daily_messages.append(now)
                self.monthly_messages.append(now)
                return True
        except Exception as e:
            logger.error(f"Failed to publish tweet: {str(e)}")
        return False

    def _clean_message_history(self, now: datetime) -> None:
        day_ago = now - timedelta(days=1)
        month_ago = now - timedelta(days=30)

        while self.daily_messages and self.daily_messages[0] < day_ago:
            self.daily_messages.popleft()
        while self.monthly_messages and self.monthly_messages[0] < month_ago:
            self.monthly_messages.popleft()
