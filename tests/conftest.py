import pytest
from datetime import datetime
from decimal import Decimal
from src.models.trade import Trade
from src.models.message import Message
from src.sources.base import TradeSource
from src.sinks.base import MessageSink


class MockTradeSource(TradeSource):
    def __init__(self, source_id: str, trades: list):
        super().__init__(source_id)
        self.trades = trades
        self.connected = False

    def connect(self) -> bool:
        self.connected = True
        return True

    def get_recent_trades(self, since: datetime):
        for trade in self.trades:
            if trade.timestamp >= since:
                yield trade

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
def sample_trade():
    return Trade(
        symbol="AAPL",
        quantity=Decimal("100"),
        price=Decimal("150.25"),
        side="BUY",
        timestamp=datetime(2024, 1, 1, 12, 0),
        source_id="test-source",
        trade_id="test-trade-1",
    )


@pytest.fixture
def matching_trade():
    return Trade(
        symbol="AAPL",
        quantity=Decimal("100"),
        price=Decimal("160.25"),
        side="SELL",
        timestamp=datetime(2024, 1, 1, 14, 30),
        source_id="test-source",
        trade_id="test-trade-2",
    )


@pytest.fixture
def mock_source(sample_trade):
    return MockTradeSource("test-source", [sample_trade])


@pytest.fixture
def mock_sink():
    return MockMessageSink("test-sink")
