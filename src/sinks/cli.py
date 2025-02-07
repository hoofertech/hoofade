import logging

from formatters.message_splitter import MessageSplitter
from models.message import Message

from .base import MessageSink

logger = logging.getLogger(__name__)


class CLISink(MessageSink):
    def __init__(self, sink_id: str):
        super().__init__(sink_id)

    def can_publish(self) -> bool:
        return True

    async def publish(self, message: Message) -> bool:
        try:
            tweets = MessageSplitter.split_to_tweets(message)

            # Print a separator for thread clarity in CLI
            print("\n" + "=" * 40 + "\n")

            for tweet in tweets:
                print(tweet.content)
                print()  # Empty line between tweets

            return True
        except Exception as e:
            logger.error(f"Error in CLI sink: {str(e)}")
            return False
