import logging
from datetime import datetime
from typing import List

from models.position import Position
from models.trade import Trade
from utils.datetime_utils import format_datetime

from .message_publisher import MessagePublisher

logger = logging.getLogger(__name__)


class CLISink(MessagePublisher):
    def __init__(self, sink_id: str):
        MessagePublisher.__init__(self, sink_id)

    def can_publish(self, message_type: str | None = None) -> bool:
        return True

    async def publish_trades(self, trades: List[Trade], now: datetime) -> bool:
        try:
            if not trades:
                return True

            message = await self.create_trade_message(trades, now)
            if message is None:
                return True

            tweets = self.split_message(message)

            # Print a separator for thread clarity in CLI
            print("\n" + "=" * 40 + "\n")
            print(f"Trade Update at {format_datetime(now)}")
            print("\n" + "-" * 40 + "\n")

            for tweet in tweets:
                print(tweet.content)
                print()  # Empty line between tweets

            return True
        except Exception as e:
            logger.error(f"Error in CLI sink: {str(e)}")
            return False

    async def publish_portfolio(self, positions: List[Position], now: datetime) -> bool:
        try:
            message = self.create_portfolio_message(positions, now)
            tweets = self.split_message(message)

            # Print a separator for thread clarity in CLI
            print("\n" + "=" * 40 + "\n")
            print(f"Portfolio Update at {format_datetime(now)}")
            print("\n" + "-" * 40 + "\n")

            for tweet in tweets:
                print(tweet.content)
                print()  # Empty line between tweets

            return True
        except Exception as e:
            logger.error(f"Error in CLI sink: {str(e)}")
            return False
