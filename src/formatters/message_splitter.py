from typing import List
from models.message import Message


class MessageSplitter:
    MAX_TWEET_LENGTH = 280
    THREAD_MARKER = " 🧵"
    CONTINUATION_MARKER = "..."
    REPO_LINK = "\n\n🚀 Build yours: github.com/hoofertech/hoofade"
    # Alternative options:
    # REPO_LINK = "\n\n🤖 Get your own bot: github.com/hoofertech/hoofade"
    # REPO_LINK = "\n\n⚡️ Clone me: github.com/hoofertech/hoofade"
    # REPO_LINK = "\n\n🔧 DIY: github.com/hoofertech/hoofade"

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

        # Second pass: add thread markers and possibly repo link
        total_tweets = len(tweets)
        final_tweets = []

        for i, tweet_content in enumerate(tweets):
            position = i + 1
            is_last_tweet = position == total_tweets

            # Calculate remaining space in last tweet
            if is_last_tweet:
                position_marker = f" ({position}/{total_tweets})"
                remaining_space = (
                    MessageSplitter.MAX_TWEET_LENGTH
                    - len(tweet_content)
                    - len(position_marker)
                )

                # Add repo link if there's space
                if remaining_space >= len(MessageSplitter.REPO_LINK):
                    tweet_content += MessageSplitter.REPO_LINK

            # Add thread markers
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
