import logging
from datetime import datetime
from typing import List

import tweepy

from config import default_timezone
from database import Database
from formatters.message_splitter import MessageSplitter
from formatters.portfolio import PortfolioFormatter
from formatters.trade import TradeFormatter
from models.message import Message
from models.position import Position
from models.trade import Trade

from .message_publisher import MessagePublisher

logger = logging.getLogger(__name__)


class TwitterSink(MessagePublisher):
    def __init__(
        self,
        sink_id: str,
        bearer_token: str,
        api_key: str,
        api_secret: str,
        access_token: str,
        access_token_secret: str,
        db: Database,
    ):
        MessagePublisher.__init__(self, sink_id, db)
        self.client = tweepy.Client(
            bearer_token=bearer_token,
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )
        self.trade_formatter = TradeFormatter()
        self.portfolio_formatter = PortfolioFormatter()
        self.last_portfolio_publish = datetime.fromtimestamp(0, tz=default_timezone())
        self.last_trade_publish = datetime.fromtimestamp(0, tz=default_timezone())

    def can_publish(self, message_type: str | None = None) -> bool:
        now = datetime.now(default_timezone())
        logger.info(f"Checking if we can publish: {message_type}")
        if message_type == "pfl":
            logger.info(
                f"Checking if we can publish portfolio: {now - self.last_portfolio_publish}"
            )
            return (now - self.last_portfolio_publish).total_seconds() >= 1800  # 30 minutes
        elif message_type == "trd":
            logger.info(f"Checking if we can publish trade batch: {now - self.last_trade_publish}")
            return (now - self.last_trade_publish).total_seconds() >= 300  # 5 minutes
        return True  # For other message types

    async def publish_trades(self, trades: List[Trade], now: datetime) -> bool:
        if not trades:
            return True

        if not self.can_publish("trd"):
            return False

        message = await self.create_trade_message(trades, now)
        if message is None:
            return True

        return await self._publish_to_twitter(message, "trd")

    async def publish_portfolio(self, positions: List[Position], now: datetime) -> bool:
        if not self.can_publish("pfl"):
            return False

        message = self.create_portfolio_message(positions, now)
        return await self._publish_to_twitter(message, "pfl")

    async def _publish_to_twitter(self, message: Message, message_type: str) -> bool:
        try:
            tweets = MessageSplitter.split_to_tweets(message)
            previous_tweet_id = None

            for tweet in tweets:
                response = self.client.create_tweet(
                    text=tweet.content,
                    in_reply_to_tweet_id=previous_tweet_id,
                )
                if isinstance(response, tweepy.Response) and response.data:
                    tweet_data = response.data
                    if tweet_data:
                        previous_tweet_id = tweet_data.get("id", None)
                    else:
                        logger.error("No tweet data in response")
                        return False
                else:
                    return False

            self._update_last_publish_time(message_type)
            return True
        except Exception as e:
            logger.error(f"Error in Twitter sink: {str(e)}")
            return False

    def _update_last_publish_time(self, message_type: str):
        now = datetime.now(default_timezone())
        if message_type == "pfl":
            self.last_portfolio_publish = now
        elif message_type == "trd":
            self.last_trade_publish = now
