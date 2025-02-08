import logging
from datetime import datetime
from typing import Optional

import tweepy

from config import default_timezone
from formatters.message_splitter import MessageSplitter
from models.message import Message

from .base import MessageSink

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
        self.last_portfolio_publish = datetime.fromtimestamp(0, tz=default_timezone())
        self.last_trade_publish = datetime.fromtimestamp(0, tz=default_timezone())

    def can_publish(self, message_type: str | None = None) -> bool:
        now = datetime.now(default_timezone())
        logger.info(f"Checking if we can publish: {message_type}")
        if message_type == "portfolio":
            logger.info(
                f"Checking if we can publish portfolio: {now - self.last_portfolio_publish}"
            )
            return (now - self.last_portfolio_publish).total_seconds() >= 1800  # 30 minutes
        elif message_type == "trade_batch":
            logger.info(f"Checking if we can publish trade batch: {now - self.last_trade_publish}")
            return (now - self.last_trade_publish).total_seconds() >= 300  # 5 minutes
        return True  # For other message types

    async def publish(self, message: Message) -> bool:
        try:
            message_type = message.metadata.get("type") if message.metadata else None
            if not self.can_publish(message_type):
                return False

            tweets = MessageSplitter.split_to_tweets(message)
            previous_tweet_id: Optional[str] = None

            for tweet in tweets:
                logger.info(f"Publishing tweet: {tweet.content}")
                try:
                    response = self.client.create_tweet(
                        text=tweet.content,
                        in_reply_to_tweet_id=previous_tweet_id,
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

            # Update the appropriate last publish time
            now = datetime.now(default_timezone())
            if message_type == "portfolio":
                self.last_portfolio_publish = now
            elif message_type == "trade_batch":
                self.last_trade_publish = now

            return True
        except Exception as e:
            logger.error(f"Error in Twitter sink: {str(e)}")
            return False
