from typing import List
from models.message import Message


class MessageSplitter:
    MAX_TWEET_LENGTH = 280
    THREAD_MARKER = " 🧵"
    CONTINUATION_MARKER = "..."

    @staticmethod
    def split_to_tweets(message: Message) -> List[Message]:
        content = message.content
        lines = content.split("\n")
        tweets = []
        current_tweet = []
        current_length = 0

        # First pass: split into tweets without markers
        for line in lines:
            line_length = len(line)

            # Account for newline character
            if current_tweet:
                line_length += 1

            # Leave room for thread markers that will be added later
            marker_space = 10  # Enough space for " 🧵 (1/10)"
            available_length = MessageSplitter.MAX_TWEET_LENGTH - marker_space

            if current_length + line_length <= available_length:
                current_tweet.append(line)
                current_length += line_length
            else:
                if current_tweet:
                    tweets.append("\n".join(current_tweet))
                current_tweet = [line]
                current_length = line_length

        # Add the last tweet if there's content
        if current_tweet:
            tweets.append("\n".join(current_tweet))

        # Second pass: add thread markers
        total_tweets = len(tweets)
        final_tweets = []

        for i, tweet_content in enumerate(tweets):
            position = i + 1

            # Add appropriate markers based on position
            if total_tweets > 1:
                if position == 1:
                    tweet_content += (
                        f"{MessageSplitter.THREAD_MARKER} (1/{total_tweets})"
                    )
                else:
                    tweet_content += f" ({position}/{total_tweets})"

            final_tweets.append(
                Message(
                    content=tweet_content,
                    timestamp=message.timestamp,
                    metadata={
                        **message.metadata,
                        "thread_position": position,
                        "total_tweets": total_tweets,
                    },
                )
            )

        return final_tweets
