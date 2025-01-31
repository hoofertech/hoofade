import tweepy
import logging
from typing import Optional
from .base import MessageSink
from models.message import Message
from formatters.message_splitter import MessageSplitter
from datetime import datetime, timezone

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
        return (now - self.last_publish).total_seconds() >= 1800  # 30 minutes

    async def publish(self, message: Message) -> bool:
        try:
            if not self.can_publish():
                return False

            tweets = MessageSplitter.split_to_tweets(message)
            previous_tweet_id: Optional[str] = None

            for tweet in tweets:
                logger.info(f"Publishing tweet: {tweet.content}")
                try:
                    response = self.client.create_tweet(
                        text=tweet.content, in_reply_to_tweet_id=previous_tweet_id
                    )
                    logger.info(f"Response: {response}")
                    # Tweepy Response object contains a data dict with tweet info
                    if isinstance(response, tweepy.Response):
                        tweet_data = response.data
                        if tweet_data:
                            previous_tweet_id = tweet_data.get("id", None)
                        else:
                            logger.error("No tweet data in response")
                            return False
                    logger.debug(f"Published tweet: {tweet.content}")
                except Exception as e:
                    logger.error(f"Error publishing tweet: {str(e)}")
                    return False

            self.last_publish = datetime.now(timezone.utc)
            return True
        except Exception as e:
            logger.error(f"Error in Twitter sink: {str(e)}")
            return False
