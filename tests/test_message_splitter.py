from datetime import datetime, timezone
from models.message import Message
from formatters.message_splitter import MessageSplitter
import logging

logger = logging.getLogger(__name__)


def test_short_message_no_split():
    message = Message(
        content="🚨 Buy $AAPL 100@$150.25",
        timestamp=datetime.now(timezone.utc),
        metadata={"type": "trade"},
    )

    tweets = MessageSplitter.split_to_tweets(message)
    assert len(tweets) == 1
    assert tweets[0].content == message.content
    assert "🧵" not in tweets[0].content


def test_portfolio_message_split():
    # Create a long portfolio with many positions to force splitting
    stocks = []
    options = []
    
    # Add many stock positions
    for i in range(20):
        stocks.append(f"$STOCK{i:02d} +{100+i}@${150.25+i:.2f}")
    
    # Add many option positions
    for i in range(20):
        options.append(f"$OPT{i:02d} {15+i}JUN24 ${150+i}C +{2+i}@${3.50+i:.2f}")
    
    content = (
        "📊 Stocks:\n" + 
        "\n".join(stocks) + 
        "\n\n🎯 Options:\n" + 
        "\n".join(options)
    )

    message = Message(
        content=content,
        timestamp=datetime.now(timezone.utc),
        metadata={"type": "portfolio"},
    )

    tweets = MessageSplitter.split_to_tweets(message)
    
    # Log tweets for debugging
    for i, tweet in enumerate(tweets):
        logger.info(f"Tweet {i+1}: {tweet.content}")
    
    # Verify splitting occurred
    assert len(tweets) > 1
    assert "🧵" in tweets[0].content
    assert "(1/" in tweets[0].content
    assert f"({len(tweets)}/{len(tweets)})" in tweets[-1].content
    
    # Verify no records are split
    for tweet in tweets:
        lines = tweet.content.split('\n')
        for line in lines:
            if line.startswith('$'):
                assert '@$' in line  # Complete record should have price


def test_metadata_preservation():
    message = Message(
        content="Test content that will not be split",
        timestamp=datetime.now(timezone.utc),
        metadata={"type": "trade", "trade_id": "123"},
    )

    tweets = MessageSplitter.split_to_tweets(message)
    assert tweets[0].metadata["type"] == "trade"
    assert tweets[0].metadata["trade_id"] == "123"
    assert tweets[0].metadata["thread_position"] == 1


def test_no_record_splitting():
    content = "🎯 Options:\n$AAPL 15JUN24 $150C +2@$3.50\n$AAPL 20SEP24 $160C -1@$2.75"

    message = Message(
        content=content,
        timestamp=datetime.now(timezone.utc),
        metadata={"type": "portfolio"},
    )

    tweets = MessageSplitter.split_to_tweets(message)
    for tweet in tweets:
        # Check that no option record is split across tweets
        lines = tweet.content.split("\n")
        for line in lines:
            if line.startswith("$"):
                assert "@" in line  # Complete record should have @ symbol
