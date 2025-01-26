import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from src.formatters.trade import TradeFormatter
from src.models.trade import Trade


@pytest.fixture
def formatter():
    return TradeFormatter()


def test_format_new_trade(formatter, sample_trade):
    message = formatter.format_trade(sample_trade)

    expected_content = "New Trade Alert 🚨\n" "$AAPL\n" "📈 Bought 100 shares @ $150.25"

    assert message.content == expected_content
    assert message.timestamp == sample_trade.timestamp
    assert message.metadata["trade_id"] == sample_trade.trade_id


def test_format_closed_position_profit(formatter, sample_trade, matching_trade):
    message = formatter.format_trade(matching_trade, sample_trade)

    expected_content = (
        "Position Closed 🎯\n" "$AAPL\n" "P&L: 6.65%\n" "Hold time: 2 hours 30 minutes"
    )

    assert message.content == expected_content
    assert message.metadata["trade_id"] == matching_trade.trade_id
    assert message.metadata["matching_trade_id"] == sample_trade.trade_id


def test_format_closed_position_loss(formatter, sample_trade):
    loss_trade = Trade(
        symbol="AAPL",
        quantity=Decimal("100"),
        price=Decimal("140.25"),
        side="SELL",
        timestamp=datetime(2024, 1, 2, 12, 0),
        source_id="test-source",
        trade_id="test-trade-3",
    )

    message = formatter.format_trade(loss_trade, sample_trade)

    expected_content = (
        "Position Closed 📊\n" "$AAPL\n" "P&L: -6.65%\n" "Hold time: 1 day"
    )

    assert message.content == expected_content


def test_hold_time_formatting(formatter):
    test_cases = [
        (timedelta(minutes=30), "30 minutes"),
        (timedelta(hours=2), "2 hours"),
        (timedelta(hours=2, minutes=30), "2 hours 30 minutes"),
        (timedelta(days=1), "1 day"),
        (timedelta(days=2), "2 days"),
        (timedelta(days=1, hours=6), "1 day 6 hours"),
    ]

    for delta, expected in test_cases:
        assert formatter._format_hold_time(delta) == expected
