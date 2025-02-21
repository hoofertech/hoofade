import logging
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
import tweepy

from config import default_timezone
from models.instrument import Instrument, InstrumentType
from models.position import Position
from models.trade import Trade
from sinks.cli import CLISink

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_twitter_sink_publish_success(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock(
            return_value=tweepy.Response(data={"id": 123}, includes={}, errors=[], meta={})
        )
        twitter_sink.client = mock_client

        now = datetime.now(default_timezone())
        trades = [create_test_trade()]

        assert await twitter_sink.publish_trades(trades, now)
        mock_client.create_tweet.assert_called_once()


@pytest.mark.asyncio
async def test_twitter_sink_publish_failure(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock(side_effect=Exception("API Error"))
        twitter_sink.client = mock_client

        now = datetime.now(default_timezone())
        trades = [create_test_trade()]  # Helper function to create a test trade

        assert not await twitter_sink.publish_trades(trades, now)


@pytest.mark.asyncio
async def test_twitter_sink_portfolio_publish_failure(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock(side_effect=Exception("API Error"))
        twitter_sink.client = mock_client

        now = datetime.now(default_timezone())
        positions = [create_test_position()]  # Helper function to create a test position

        assert not await twitter_sink.publish_portfolio(positions, now)


@pytest.mark.asyncio
async def test_twitter_sink_rate_limit(twitter_sink):
    with patch("tweepy.Client") as mock_client:
        mock_client.create_tweet = Mock(
            return_value=tweepy.Response(data={"id": 123}, includes={}, errors=[], meta={})
        )
        twitter_sink.client = mock_client
        now = datetime.now(default_timezone())
        trades = [create_test_trade()]

        # First message should succeed
        assert await twitter_sink.publish_trades(trades, now)

        # Second message should fail due to rate limit
        assert not await twitter_sink.publish_trades(trades, now)

        mock_client.create_tweet.assert_called_once()


@pytest.fixture
def cli_sink(db_session):
    return CLISink(sink_id="test-cli", db=db_session)


@pytest.mark.asyncio
async def test_cli_sink_publish_success(cli_sink, capsys):
    now = datetime.now(default_timezone())
    trades = [create_test_trade()]

    assert await cli_sink.publish_trades(trades, now)
    captured = capsys.readouterr()
    assert "Trade Update" in captured.out


@pytest.mark.asyncio
async def test_cli_sink_always_can_publish(cli_sink):
    assert cli_sink.can_publish("trd") is True
    assert cli_sink.can_publish("pfl") is True


def create_test_position():
    instrument = create_test_instrument()
    return Position(
        instrument=instrument,
        quantity=Decimal("100"),
        cost_basis=Decimal("150.25"),
        market_price=Decimal("155.00"),
        report_time=datetime.now(default_timezone()),
    )


def create_test_trade():
    return Trade(
        instrument=create_test_instrument(),
        quantity=Decimal("100"),
        price=Decimal("150.25"),
        side="BUY",
        timestamp=datetime.now(default_timezone()),
        source_id="test-source",
        trade_id="test-trade-1",
        currency="USD",
    )


def create_test_instrument():
    return Instrument(
        symbol="AAPL",
        currency="USD",
        type=InstrumentType.STOCK,
        option_details=None,
    )
