import pytest
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from models.message import Message
from models.trade import Trade
from models.instrument import Instrument, OptionType, InstrumentType
from formatters.trade import TradeFormatter
from sinks.cli import CLISink
from sources.ibkr_json_source import JsonSource
from pathlib import Path
logger = logging.getLogger(__name__)

@pytest.fixture
def test_data_dir():
    return Path("tests/test_data")

@pytest.fixture
def cli_sink():
    return CLISink("test-cli-sink")

@pytest.fixture
def trade_formatter():
    return TradeFormatter()

@pytest.fixture
async def json_source(test_data_dir):
    source = JsonSource(
        source_id="test-source",
        data_dir=str(test_data_dir)
    )
    await source.connect()
    yield source
    await source.disconnect()

@pytest.mark.asyncio
async def test_cli_sink_stock_trade(cli_sink, json_source, trade_formatter, capsys):
    # Get BABA position from source
    positions = await json_source.get_positions()
    baba_position = next(p for p in positions if p.instrument.symbol == "BABA")
    
    # Create a trade
    trade = Trade(
        instrument=baba_position.instrument,
        quantity=Decimal("100"),
        price=Decimal("96.03"),
        side="BUY",
        timestamp=datetime.now(timezone.utc),
        source_id="test-source",
        trade_id="test-trade-1"
    )
    
    # Format trade using formatter
    message = trade_formatter.format_trade(trade)
    
    # Publish to CLI sink
    await cli_sink.publish(message)
    
    # Check output
    captured = capsys.readouterr()
    logger.info("captured.out: %s", captured.out)
    assert "Buy" in captured.out
    assert "$BABA" in captured.out
    assert "100 shares" in captured.out
    assert "$96.03" in captured.out

@pytest.mark.asyncio
async def test_cli_sink_option_trade(cli_sink, json_source, trade_formatter, capsys):
    # Get NVDA option position from source
    positions = await json_source.get_positions()
    nvda_position = next(p for p in positions if p.instrument.symbol == "NVDA" and p.instrument.option_details is not None)
    
    # Create an option trade
    trade = Trade(
        instrument=nvda_position.instrument,
        quantity=Decimal("1"),
        price=Decimal("4.37"),
        side="BUY",
        timestamp=datetime.now(timezone.utc),
        source_id="test-source",
        trade_id="test-trade-2"
    )
    
    # Format trade using formatter
    message = trade_formatter.format_trade(trade)
    
    # Publish to CLI sink
    await cli_sink.publish(message)
    
    # Check output
    captured = capsys.readouterr()
    logger.info("captured.out: %s", captured.out)
    assert "Buy" in captured.out
    assert "$NVDA" in captured.out
    assert "18 Jul 2025" in captured.out
    assert "100P" in captured.out
    assert "1 contract" in captured.out
    assert "$4.37" in captured.out

@pytest.mark.asyncio
async def test_cli_sink_closing_trade(cli_sink, json_source, trade_formatter, capsys):
    # Get PLTR short position from source
    positions = await json_source.get_positions()
    pltr_position = next(p for p in positions if p.instrument.symbol == "PLTR")
    
    # Create opening trade
    opening_trade = Trade(
        instrument=pltr_position.instrument,
        quantity=Decimal("-200"),
        price=Decimal("76.59"),
        side="SELL",
        timestamp=datetime.now(timezone.utc) - timedelta(hours=2),
        source_id="test-source",
        trade_id="test-trade-3"
    )
    
    # Create closing trade
    closing_trade = Trade(
        instrument=pltr_position.instrument,
        quantity=Decimal("200"),
        price=Decimal("80.23"),
        side="BUY",
        timestamp=datetime.now(timezone.utc),
        source_id="test-source",
        trade_id="test-trade-4"
    )
    
    # Format trade using formatter with matching trade
    message = trade_formatter.format_trade(closing_trade, opening_trade)
    
    # Publish to CLI sink
    await cli_sink.publish(message)
    
    # Check output
    captured = capsys.readouterr()
    logger.info("captured.out: %s", captured.out)
    assert "Closed" in captured.out
    assert "$PLTR" in captured.out
    assert "4.75%" in captured.out
    assert "2 hours" in captured.out

@pytest.mark.asyncio
async def test_cli_sink_real_trades(cli_sink, json_source, trade_formatter, capsys):
    """Test using actual trades from the JSON file"""
    # Get META trades from the test data
    positions = await json_source.get_positions()
    meta_position = next(p for p in positions if p.instrument.symbol == "META" and p.instrument.option_details is not None)
    
    # Create trades based on actual data from JSON
    opening_trade = Trade(
        instrument=meta_position.instrument,
        quantity=Decimal("2"),
        price=Decimal("9.20"),
        side="BUY",
        timestamp=datetime(2025, 1, 29, 11, 23, 9, tzinfo=timezone.utc),
        source_id="test-source",
        trade_id="466919725"
    )
    
    closing_trade = Trade(
        instrument=meta_position.instrument,
        quantity=Decimal("-2"),
        price=Decimal("4.92"),
        side="SELL",
        timestamp=datetime(2025, 1, 29, 15, 10, 39, tzinfo=timezone.utc),
        source_id="test-source",
        trade_id="467074826"
    )
    
    # Format and publish opening trade
    message = trade_formatter.format_trade(opening_trade)
    await cli_sink.publish(message)
    
    # Format and publish closing trade
    message = trade_formatter.format_trade(closing_trade, opening_trade)
    await cli_sink.publish(message)
    
    # Check output
    captured = capsys.readouterr()
    logger.info("captured.out: %s", captured.out)
    output = captured.out
    
    # Verify opening trade
    assert "Buy" in output
    assert "$META" in output
    assert "715C" in output
    assert "2 contracts" in output
    assert "$9.20" in output
    
    # Verify closing trade
    assert "Closed" in output
    assert "46.52%" in output
    assert "3 hours" in output
