import pytest
from datetime import timedelta, date
from decimal import Decimal
from formatters.trade import TradeFormatter
from models.trade import Trade
from models.instrument import Instrument, OptionType


@pytest.fixture
def formatter():
    return TradeFormatter()


@pytest.fixture
def stock_instrument():
    return Instrument.stock(symbol="AAPL")


@pytest.fixture
def call_option_instrument():
    return Instrument.option(
        symbol="AAPL",
        strike=Decimal("150"),
        expiry=date(2024, 6, 15),
        option_type=OptionType.CALL,
    )


@pytest.fixture
def put_option_instrument():
    return Instrument.option(
        symbol="AAPL",
        strike=Decimal("140"),
        expiry=date(2024, 6, 15),
        option_type=OptionType.PUT,
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
    message = formatter.format_trade(stock_trade)

    expected_content = "🚨 Buy $AAPL: 100@$150.25"

    assert message.content == expected_content
    assert message.timestamp == stock_trade.timestamp
    assert message.metadata["trade_id"] == stock_trade.trade_id


def test_format_new_call_option_trade(formatter, call_option_trade):
    message = formatter.format_trade(call_option_trade)

    expected_content = "🚨 Buy $AAPL/15-Jun-2024@$150C: 5@$3.50"

    assert message.content == expected_content
    assert message.timestamp == call_option_trade.timestamp
    assert message.metadata["trade_id"] == call_option_trade.trade_id


def test_format_closed_stock_position_profit(formatter, stock_trade):
    matching_trade = Trade(
        instrument=stock_trade.instrument,
        quantity=Decimal("-100"),
        price=Decimal("160.25"),
        side="SELL",
        currency="USD",
        timestamp=stock_trade.timestamp + timedelta(hours=2, minutes=30),
        source_id="test-source",
        trade_id="test-exec-id-3",
    )

    message = formatter.format_trade(matching_trade, stock_trade)

    assert "$AAPL" in message.content
    assert "-6.66%" in message.content
    assert "2 hours 30 minutes" in message.content


def test_format_closed_option_position_loss(formatter, call_option_trade):
    matching_trade = Trade(
        instrument=call_option_trade.instrument,
        quantity=Decimal("-5"),
        price=Decimal("2.50"),
        side="SELL",
        currency="USD",
        timestamp=call_option_trade.timestamp + timedelta(days=1),
        source_id="test-source",
        trade_id="test-exec-id-4",
    )

    message = formatter.format_trade(matching_trade, call_option_trade)

    assert "$AAPL/15-Jun-2024@$150C" in message.content
    assert "28.57%" in message.content
    assert "1 day" in message.content
