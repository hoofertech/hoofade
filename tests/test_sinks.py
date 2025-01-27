import pytest
from datetime import datetime, timezone
from sinks.twitter import TwitterSink
from models.message import Message
from unittest.mock import Mock, patch


@pytest.fixture
def twitter_sink():
    config = {
        "sink_id": "test-twitter",
        "credentials": {
            "bearer_token": "test-token",
            "consumer_key": "test-key",
            "consumer_secret": "test-secret",
            "access_token": "test-access",
            "access_token_secret": "test-access-secret",
        },
    }
    return TwitterSink(**config)


def test_twitter_sink_rate_limits(twitter_sink):
    # Test daily limit
    now = datetime.now(timezone.utc)
    for _ in range(twitter_sink.MAX_TWEETS_PER_DAY):
        twitter_sink.daily_messages.append(now)

    assert not twitter_sink.can_publish()


def test_twitter_sink_publish(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        twitter_sink.client = mock_client

        mock_response = Mock()
        mock_response.data = {"id": "123456"}
        mock_client.create_tweet.return_value = mock_response

        message = Message(
            content="Test message", timestamp=datetime.now(timezone.utc), metadata={}
        )

        assert twitter_sink.publish(message)
        mock_client.create_tweet.assert_called_once_with(text="Test message")


def test_twitter_sink_publish_failure(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        twitter_sink.client = mock_client
        mock_client.create_tweet.side_effect = Exception("API Error")

        message = Message(
            content="Test message", timestamp=datetime.now(timezone.utc), metadata={}
        )

        assert not twitter_sink.publish(message)
