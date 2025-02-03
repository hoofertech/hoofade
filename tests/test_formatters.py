import pytest
from datetime import date
from decimal import Decimal
from formatters.trade import TradeFormatter
from models.trade import Trade
from models.instrument import Instrument, OptionType


@pytest.fixture
def formatter():
    return TradeFormatter()


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
def stock_trade(test_timestamp, stock_instrument):
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
def call_option_trade(test_timestamp, call_option_instrument):
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


def test_format_new_stock_trade(formatter, stock_trade):
    message = formatter._format_trade(stock_trade)

    expected_content = "🚨 BUY  $AAPL 100@$150.25"

    assert message.content == expected_content
    assert message.timestamp == stock_trade.timestamp
    assert message.metadata["trade_id"] == stock_trade.trade_id


def test_format_new_call_option_trade(formatter, call_option_trade):
    message = formatter._format_trade(call_option_trade)

    expected_content = "🚨 BUY  $AAPL 15JUN24 $150C 5@$3.50"

    assert message.content == expected_content
    assert message.timestamp == call_option_trade.timestamp
    assert message.metadata["trade_id"] == call_option_trade.trade_id
