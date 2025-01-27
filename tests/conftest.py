import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from src.models.trade import Trade
from src.models.message import Message
from src.sources.base import TradeSource
from src.sinks.base import MessageSink
import logging

logger = logging.getLogger(__name__)


class MockTradeSource(TradeSource):
    def __init__(self, source_id: str, trades: list):
        super().__init__(source_id)
        self.trades = trades
        self.connected = False

    def connect(self) -> bool:
        self.connected = True
        return True

    def get_recent_trades(self, since: datetime):
        return iter([t for t in self.trades if t.timestamp >= since])

    def disconnect(self) -> None:
        self.connected = False


class MockMessageSink(MessageSink):
    def __init__(self, sink_id: str):
        super().__init__(sink_id)
        self.messages = []
        self.connected = False
        self.can_publish_result = True

    def connect(self) -> bool:
        self.connected = True
        return True

    def publish(self, message: Message) -> bool:
        if self.can_publish():
            self.messages.append(message)
            return True
        return False

    def can_publish(self) -> bool:
        return self.can_publish_result


@pytest.fixture
def mock_source(sample_trade):
    return MockTradeSource("test-source", [sample_trade])


@pytest.fixture
def mock_sink():
    return MockMessageSink("test-sink")


@pytest.fixture
def test_timestamp():
    """Fixed timestamp for testing"""
    return datetime(2024, 3, 20, 14, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def sample_trade(test_timestamp):
    logger.info(f"type(test_timestamp): {test_timestamp}")
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
    logger.info(f"type(later_timestamp): {later_timestamp}")
    return Trade(
        symbol="AAPL",
        quantity=Decimal("-100"),
        price=Decimal("160.25"),
        side="SELL",
        timestamp=later_timestamp,
        source_id="test-source",
        trade_id="test-exec-id-2",
    )
