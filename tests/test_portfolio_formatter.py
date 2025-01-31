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
            quantity=Decimal("-900"),  # Short position
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
            quantity=Decimal("-1"),  # Short position
            market_price=Decimal("1.78"),
            cost_basis=Decimal("1.80"),
        ),
    ]
    return positions


def test_format_portfolio(portfolio_formatter, sample_positions):
    timestamp = datetime.now(timezone.utc)
    message = portfolio_formatter.format_portfolio(sample_positions, timestamp)

    logger.info(f"message: {message.content}")

    # Check content structure
    lines = message.content.split("\n")
    assert lines[0] == "📊 Stocks:"
    assert "$BABA +600@$96.03" in lines[1]
    assert "$UBI  -900@€11.71" in lines[2]
    assert lines[3] == ""  # Blank line between sections
    assert lines[4] == "🎯 Options:"
    assert "$NVDA 18JUL25 $100   P +1@$4.37" in lines[5]
    assert "$TWLO 31JAN25 $150   C -1@$1.78" in lines[6]

    # Check metadata
    assert message.metadata["type"] == "portfolio"


def test_empty_portfolio(portfolio_formatter):
    timestamp = datetime.now(timezone.utc)
    message = portfolio_formatter.format_portfolio([], timestamp)
    assert message.content == "No positions"
    assert message.metadata["type"] == "portfolio"
