import pytest
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from models.trade import Trade
from models.instrument import Instrument, OptionType
from models.message import Message
from sources.base import TradeSource
from sinks.base import MessageSink
from sinks.twitter import TwitterSink
from typing import AsyncIterator
from models.position import Position


class MockTradeSource(TradeSource):
    def __init__(self, source_id: str, trades: list[Trade]):
        super().__init__(source_id)
        self.trades = trades
        self.positions = []

    async def get_positions(self) -> AsyncIterator[Position]:
        for position in self.positions:
            yield position

    async def get_last_day_trades(self) -> AsyncIterator[Trade]:
        for trade in self.trades:
            yield trade

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        pass


class MockMessageSink(MessageSink):
    def __init__(self, sink_id: str):
        super().__init__(sink_id)
        self.messages: list[Message] = []

    async def publish(self, message: Message) -> bool:
        self.messages.append(message)
        return True

    def can_publish(self) -> bool:
        return True


@pytest.fixture
def test_timestamp():
    """Fixed timestamp for testing"""
    return datetime(2024, 3, 20, 14, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def stock_instrument():
    return Instrument.stock(symbol="AAPL", currency="USD")


@pytest.fixture
def call_option_instrument():
    return Instrument.option(
        symbol="AAPL",
        strike=Decimal("150"),
        expiry=date(2024, 6, 15),
        option_type=OptionType.CALL,
        currency="USD",
    )


@pytest.fixture
def put_option_instrument():
    return Instrument.option(
        symbol="AAPL",
        strike=Decimal("140"),
        expiry=date(2024, 6, 15),
        option_type=OptionType.PUT,
        currency="USD",
    )


@pytest.fixture
def sample_trade(test_timestamp, stock_instrument):
    """Sample stock trade with fixed timestamp"""
    return Trade(
        instrument=stock_instrument,
        quantity=Decimal("100"),
        price=Decimal("150.25"),
        side="BUY",
        currency="USD",
        timestamp=test_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-1",
    )


@pytest.fixture
def sample_option_trade(test_timestamp, call_option_instrument):
    """Sample option trade with fixed timestamp"""
    return Trade(
        instrument=call_option_instrument,
        quantity=Decimal("5"),
        price=Decimal("3.50"),
        side="BUY",
        currency="USD",
        timestamp=test_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-2",
    )


@pytest.fixture
def matching_trade(test_timestamp, stock_instrument):
    """Matching stock trade with fixed timestamp + 2.5 hours"""
    later_timestamp = test_timestamp + timedelta(hours=2, minutes=30)
    return Trade(
        instrument=stock_instrument,
        quantity=Decimal("-100"),
        price=Decimal("160.25"),
        side="SELL",
        currency="USD",
        timestamp=later_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-3",
    )


@pytest.fixture
def matching_option_trade(test_timestamp, call_option_instrument):
    """Matching option trade with fixed timestamp + 2.5 hours"""
    later_timestamp = test_timestamp + timedelta(hours=2, minutes=30)
    return Trade(
        instrument=call_option_instrument,
        quantity=Decimal("-5"),
        price=Decimal("4.50"),
        side="SELL",
        currency="USD",
        timestamp=later_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-4",
    )


@pytest.fixture
def mock_source(sample_trade):
    return MockTradeSource("test-source", [sample_trade])


@pytest.fixture
def mock_option_source(sample_option_trade):
    return MockTradeSource("test-source", [sample_option_trade])


@pytest.fixture
def mock_sink():
    return MockMessageSink("test-sink")


@pytest.fixture
def twitter_sink():
    """Twitter sink with test credentials"""
    return TwitterSink(
        sink_id="test-twitter",
        bearer_token="test-bearer-token",
        api_key="test-api-key",
        api_secret="test-api-secret",
        access_token="test-access-token",
        access_token_secret="test-access-token-secret",
    )
