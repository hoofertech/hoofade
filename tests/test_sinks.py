import pytest
from datetime import datetime, timezone
from unittest.mock import patch, Mock
from models.message import Message
from sinks.cli import CLISink


@pytest.mark.asyncio
async def test_twitter_sink_publish_success(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock()
        twitter_sink.client = mock_client

        message = Message(
            content="Test message", timestamp=datetime.now(timezone.utc), metadata={}
        )

        assert await twitter_sink.publish(message)
        mock_client.create_tweet.assert_called_once_with(text="Test message", in_reply_to_tweet_id=None)


@pytest.mark.asyncio
async def test_twitter_sink_publish_failure(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock(side_effect=Exception("API Error"))
        twitter_sink.client = mock_client

        message = Message(
            content="Test message", timestamp=datetime.now(timezone.utc), metadata={}
        )

        assert not await twitter_sink.publish(message)


@pytest.mark.asyncio
async def test_twitter_sink_rate_limit(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock()
        twitter_sink.client = mock_client

        # First message should succeed
        message1 = Message(
            content="Test message 1", timestamp=datetime.now(timezone.utc), metadata={}
        )
        assert await twitter_sink.publish(message1)

        # Second message should fail due to rate limit
        message2 = Message(
            content="Test message 2", timestamp=datetime.now(timezone.utc), metadata={}
        )
        assert not await twitter_sink.publish(message2)

        mock_client.create_tweet.assert_called_once_with(text="Test message 1", in_reply_to_tweet_id=None)


@pytest.fixture
def cli_sink():
    return CLISink(sink_id="test-cli")


@pytest.mark.asyncio
async def test_cli_sink_publish_success(cli_sink, capsys):
    message = Message(
        content="Test CLI message", timestamp=datetime.now(timezone.utc), metadata={}
    )

    assert await cli_sink.publish(message)
    captured = capsys.readouterr()
    assert "Test CLI message" in captured.out


@pytest.mark.asyncio
async def test_cli_sink_always_can_publish(cli_sink):
    assert cli_sink.can_publish() is True
