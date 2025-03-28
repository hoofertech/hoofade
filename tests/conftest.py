from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, override

import pytest

from config import default_timezone
from database import Database
from models.instrument import Instrument, OptionType
from models.message import Message
from models.position import Position
from models.trade import Trade
from sinks.message_publisher import MessagePublisher
from sinks.twitter import TwitterSink
from sources.base import TradeSource


class MockTradeSource(TradeSource):
    def __init__(self, source_id: str, trades: list[dict[str, Any]]):
        super().__init__(source_id)
        self.trades = trades

    @override
    async def load_latest_positions_data(self) -> Tuple[Dict[str, Any] | None, datetime | None]:
        return None, None

    @override
    async def load_latest_trades_data(self) -> Tuple[list[dict[str, Any]] | None, datetime | None]:
        return self.trades, None

    @override
    def is_done(self) -> bool:
        return True

    @override
    def get_sleep_time(self) -> int:
        return 1


class MockMessageSink(MessagePublisher):
    def __init__(self, sink_id: str, db: Database):
        MessagePublisher.__init__(self, sink_id, db)
        self.published_trades: List[List[Trade]] = []
        self.published_portfolios: List[List[Position]] = []
        self.last_trade_timestamp: Optional[datetime] = None
        self.last_portfolio_timestamp: Optional[datetime] = None
        # For backward compatibility
        self.messages: List[Message] = []

    def can_publish(self, message_type: str | None = None) -> bool:
        return True

    async def publish_trades(self, trades: List[Trade], now: datetime) -> bool:
        if not trades:
            return True

        self.published_trades.append(trades)
        self.last_trade_timestamp = now
        # For backward compatibility
        message = await self.create_trade_message(trades, now)
        if message is not None:
            self.messages.append(message)
        return True

    async def publish_portfolio(self, positions: List[Position], now: datetime) -> bool:
        self.published_portfolios.append(positions)
        self.last_portfolio_timestamp = now
        return True


@pytest.fixture
async def db_session():
    """Create in-memory SQLite database for testing."""
    import os

    if os.path.exists("testdb.db"):
        os.remove("testdb.db")
    engine = Database("sqlite+aiosqlite:///testdb.db")

    await engine.initialize()

    yield engine

    os.remove("testdb.db")


@pytest.fixture
def test_timestamp():
    """Fixed timestamp for testing"""
    return datetime(2024, 3, 20, 14, 30, 0, tzinfo=default_timezone())


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
def sample_trade_data(test_timestamp, stock_instrument):
    """Sample stock trade with fixed timestamp"""
    return {
        "accountId": "U7170000",
        "currency": stock_instrument.currency,
        "symbol": stock_instrument.symbol,
        "listingExchange": "NYSE",
        "expiry": "",
        "putCall": "",
        "tradeID": 470724576,
        "dateTime": test_timestamp.strftime("%Y%m%d;%H%M%S"),
        "buySell": "BUY",
        "quantity": 100,
        "price": 150.25,
        "strike": "",
        "underlyingSymbol": stock_instrument.symbol,
    }


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
        trade_id="470724576",
    )


@pytest.fixture
def sample_option_trade_data(test_timestamp, call_option_instrument):
    """Sample option trade with fixed timestamp"""
    return {
        "accountId": "U7170000",
        "currency": call_option_instrument.currency,
        "symbol": call_option_instrument.symbol,
        "listingExchange": "CBOE",
        "underlyingSymbol": call_option_instrument.symbol,
        "expiry": call_option_instrument.expiry.strftime("%Y%m%d"),
        "putCall": "C" if call_option_instrument.option_type == OptionType.CALL else "P",
        "tradeID": 466929324,
        "dateTime": test_timestamp.strftime("%Y%m%d;%H%M%S"),
        "buySell": "BUY",
        "quantity": 5,
        "price": 3.50,
        "strike": call_option_instrument.strike,
    }


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
def mock_source(sample_trade_data):
    return MockTradeSource("test-source", [sample_trade_data])


@pytest.fixture
def mock_option_source(sample_option_trade_data):
    return MockTradeSource("test-source", [sample_option_trade_data])


@pytest.fixture
def mock_sink(db_session):
    return MockMessageSink("test-sink", db_session)


@pytest.fixture
def twitter_sink(db_session):
    """Twitter sink with test credentials"""
    return TwitterSink(
        sink_id="test-twitter",
        bearer_token="test-bearer-token",
        api_key="test-api-key",
        api_secret="test-api-secret",
        access_token="test-access-token",
        access_token_secret="test-access-token-secret",
        db=db_session,
    )


@pytest.fixture
def sample_positions(stock_instrument, call_option_instrument, test_timestamp):
    """Sample positions for testing"""
    return [
        Position(
            instrument=stock_instrument,
            quantity=Decimal("100"),
            market_price=Decimal("150.25"),
            cost_basis=Decimal("145.50"),
            report_time=test_timestamp,
        ),
        Position(
            instrument=call_option_instrument,
            quantity=Decimal("5"),
            market_price=Decimal("3.50"),
            cost_basis=Decimal("2.75"),
            report_time=test_timestamp,
        ),
    ]
