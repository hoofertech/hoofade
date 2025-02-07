import logging
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from formatters.portfolio import PortfolioFormatter
from models.instrument import Instrument, OptionType
from models.position import Position

logger = logging.getLogger(__name__)


@pytest.fixture
def portfolio_formatter():
    return PortfolioFormatter()


@pytest.fixture
def sample_positions(test_timestamp):
    positions = [
        Position(
            instrument=Instrument.stock(symbol="BABA", currency="USD"),
            quantity=Decimal("600"),
            market_price=Decimal("96.03"),
            cost_basis=Decimal("82.15"),
            report_time=test_timestamp,
        ),
        Position(
            instrument=Instrument.stock(symbol="UBI", currency="EUR"),
            quantity=Decimal("-900"),  # Short position
            market_price=Decimal("11.71"),
            cost_basis=Decimal("17.40"),
            report_time=test_timestamp,
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
            report_time=test_timestamp,
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
            report_time=test_timestamp,
        ),
    ]
    return positions


def test_format_portfolio(portfolio_formatter, sample_positions):
    timestamp = datetime.now(timezone.utc)
    message = portfolio_formatter.format_portfolio(sample_positions, timestamp)

    logger.info(f"message: {message.content}")

    # Check content structure
    lines = message.content.split("\n")
    expected_date = timestamp.strftime("%d %b %Y").upper()
    assert lines[0] == f"Portfolio on {expected_date}"
    assert lines[1] == ""  # Empty line after title
    assert lines[2] == "📊 Stocks:"
    assert "$BABA +600@$82.15" in lines[3]
    assert "$UBI  -900@€17.40" in lines[4]
    assert lines[5] == ""  # Blank line between sections
    assert lines[6] == "🎯 Options:"
    assert "$NVDA 18JUL25 $100   P +1@$2.8" in lines[7]
    assert "$TWLO 31JAN25 $150   C -1@$1.8" in lines[8]

    # Check metadata
    assert message.metadata["type"] == "portfolio"


def test_empty_portfolio(portfolio_formatter):
    timestamp = datetime.now(timezone.utc)
    message = portfolio_formatter.format_portfolio([], timestamp)
    assert message.content == "No positions"
    assert message.metadata["type"] == "portfolio"
