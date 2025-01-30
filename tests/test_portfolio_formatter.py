import pytest
from datetime import datetime, timezone, date
from decimal import Decimal
from models.position import Position
from models.instrument import Instrument, OptionType
from formatters.portfolio import PortfolioFormatter
import logging

logger = logging.getLogger(__name__)


@pytest.fixture
def portfolio_formatter():
    return PortfolioFormatter()


@pytest.fixture
def sample_positions():
    positions = [
        Position(
            instrument=Instrument.stock(symbol="BABA", currency="USD"),
            quantity=Decimal("600"),
            market_price=Decimal("96.03"),
            cost_basis=Decimal("82.15"),
        ),
        Position(
            instrument=Instrument.stock(symbol="UBI", currency="EUR"),
            quantity=Decimal("900"),
            market_price=Decimal("11.71"),
            cost_basis=Decimal("17.40"),
        ),
        Position(
            instrument=Instrument.option(
                symbol="NVDA",
                strike=Decimal("100"),
                expiry=date(2025, 7, 18),
                option_type=OptionType.PUT,
                currency="USD",
            ),
            quantity=Decimal("1"),
            market_price=Decimal("4.37"),
            cost_basis=Decimal("2.80"),
        ),
        Position(
            instrument=Instrument.option(
                symbol="TWLO",
                strike=Decimal("150"),
                expiry=date(2025, 1, 31),
                option_type=OptionType.CALL,
                currency="USD",
            ),
            quantity=Decimal("-1"),
            market_price=Decimal("1.78"),
            cost_basis=Decimal("1.80"),
        ),
    ]
    return positions


def test_format_portfolio(portfolio_formatter, sample_positions):
    timestamp = datetime.now(timezone.utc)
    messages = portfolio_formatter.format_portfolio(sample_positions, timestamp)

    for m in messages:
        logger.info(f"message: {m.content}")

    assert len(messages) == 2

    # Check stock message
    stock_message = messages[0]
    assert "📈 Stock Positions:" in stock_message.content
    assert "$BABA: 600@$96.03" in stock_message.content
    assert "$UBI: 900@€11.71" in stock_message.content
    assert stock_message.metadata["type"] == "stock_portfolio"

    # Check option message
    option_message = messages[1]
    assert "🎯 Option Positions:" in option_message.content
    assert "31JAN2025:" in option_message.content
    assert "$TWLO $150C: 1@$1.78" in option_message.content
    assert "18JUL2025:" in option_message.content
    assert "$NVDA $100P: 1@$4.37" in option_message.content
    assert option_message.metadata["type"] == "option_portfolio"
