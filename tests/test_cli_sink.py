import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from config import default_timezone
from formatters.trade import TradeFormatter
from models.trade import Trade
from sinks.cli import CLISink
from sources.ibkr_json_source import JsonSource

logger = logging.getLogger(__name__)


@pytest.fixture
def test_data_dir():
    return Path("tests/test_data")


@pytest.fixture
def cli_sink(db_session):
    return CLISink("test-cli-sink", db_session)


@pytest.fixture
def trade_formatter():
    return TradeFormatter()


@pytest.fixture
async def json_source(test_data_dir):
    source = JsonSource(source_id="test-source", data_dir=str(test_data_dir))
    await source.load_positions()
    yield source


@pytest.mark.asyncio
async def test_cli_sink_stock_trade(cli_sink, json_source, trade_formatter, capsys):
    # Get BABA position from source
    positions = json_source.get_positions()
    baba_position = next(p for p in positions if p.instrument.symbol == "BABA")

    # Create a trade
    trade = Trade(
        instrument=baba_position.instrument,
        quantity=Decimal("100"),
        price=Decimal("96.03"),
        side="BUY",
        timestamp=datetime.now(default_timezone()),
        source_id="test-source",
        trade_id="test-trade-1",
        currency="USD",
    )

    # Publish to CLI sink
    now = datetime.now(default_timezone())
    await cli_sink.publish_trades([trade], now)

    # Check output
    captured = capsys.readouterr()
    assert "BUY" in captured.out
    assert "$BABA" in captured.out
    assert "100" in captured.out
    assert "$96.03" in captured.out


@pytest.mark.asyncio
async def test_cli_sink_option_trade(cli_sink, json_source, trade_formatter, capsys):
    # Get NVDA option position from source
    positions = json_source.get_positions()
    nvda_position = next(
        p
        for p in positions
        if p.instrument.symbol == "NVDA" and p.instrument.option_details is not None
    )

    # Create an option trade
    trade = Trade(
        instrument=nvda_position.instrument,
        quantity=Decimal("1111"),
        price=Decimal("4.37"),
        side="BUY",
        timestamp=datetime.now(default_timezone()),
        source_id="test-source",
        trade_id="test-trade-2",
        currency="USD",
    )

    # Publish to CLI sink
    now = datetime.now(default_timezone())
    await cli_sink.publish_trades([trade], now)

    # Check output
    captured = capsys.readouterr()
    assert "BUY" in captured.out
    assert "$NVDA" in captured.out
    assert "18JUL25" in captured.out
    assert "100P" in captured.out
    assert "1111" in captured.out
    assert "$4.37" in captured.out


@pytest.mark.asyncio
async def test_cli_sink_real_trades(cli_sink, json_source, trade_formatter, capsys):
    """Test using actual trades from the JSON file"""
    # Get META trades from the test data
    positions = json_source.get_positions()
    meta_position = next(
        p
        for p in positions
        if p.instrument.symbol == "META" and p.instrument.option_details is not None
    )

    # Create trades based on actual data from JSON
    opening_trade = Trade(
        instrument=meta_position.instrument,
        quantity=Decimal("222"),
        price=Decimal("9.20"),
        side="BUY",
        timestamp=datetime(2025, 1, 29, 11, 23, 9, tzinfo=default_timezone()),
        source_id="test-source",
        trade_id="466919725",
        currency="USD",
    )

    # Publish to CLI sink
    now = datetime.now(default_timezone())
    await cli_sink.publish_trades([opening_trade], now)

    # Check output
    captured = capsys.readouterr()
    output = captured.out

    # Verify opening trade
    assert "BUY" in output
    assert "$META" in output
    assert "715C" in output
    assert "222" in output
    assert "$9.20" in output
