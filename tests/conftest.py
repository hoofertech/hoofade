from typing import List, AsyncIterator
import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import logging
from models.trade import Trade
from models.message import Message
from sources.base import TradeSource
from sinks.base import MessageSink
from sinks.twitter import TwitterSink

logger = logging.getLogger(__name__)


class MockTradeSource(TradeSource):
    def __init__(self, source_id: str, trades: List[Trade]):
        super().__init__(source_id)
        self.trades = trades
        self.connected = False

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def get_recent_trades(self, since: datetime) -> AsyncIterator[Trade]:
        for trade in self.trades:
            if trade.timestamp >= since:
                yield trade

    async def disconnect(self) -> None:
        self.connected = False


class MockMessageSink(MessageSink):
    def __init__(self, sink_id: str):
        super().__init__(sink_id)
        self.messages = []
        self.can_publish_result = True

    def can_publish(self) -> bool:
        return self.can_publish_result

    async def publish(self, message: Message) -> bool:
        if self.can_publish():
            self.messages.append(message)
            return True
        return False


@pytest.fixture
def test_timestamp():
    """Fixed timestamp for testing"""
    return datetime(2024, 3, 20, 14, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def sample_trade(test_timestamp):
    """Sample trade with fixed timestamp"""
    return Trade(
        symbol="AAPL",
        quantity=Decimal("100"),
        price=Decimal("150.25"),
        side="BUY",
        timestamp=test_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-1",
    )


@pytest.fixture
def matching_trade(test_timestamp):
    """Matching trade with fixed timestamp + 2.5 hours"""
    later_timestamp = test_timestamp + timedelta(hours=2, minutes=30)
    return Trade(
        symbol="AAPL",
        quantity=Decimal("-100"),
        price=Decimal("160.25"),
        side="SELL",
        timestamp=later_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-2",
    )


@pytest.fixture
def mock_source(sample_trade):
    return MockTradeSource("test-source", [sample_trade])


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


@pytest.fixture(autouse=True)
def setup_logging():
    """Set up logging for all tests"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
