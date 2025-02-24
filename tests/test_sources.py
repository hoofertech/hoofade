import logging
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from models.instrument import InstrumentType, OptionType
from sources.ibkr import IBKRSource

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_empty_flex_report():
    with patch("sources.flex_client.FlexReport") as mock_report_class:
        # Create a mock for the FlexReport instance
        mock_report_instance = Mock()

        # Mock the download method (not async)
        mock_report_instance.download = Mock()

        # Create mock DataFrame for positions
        positions_data = {
            "symbol": [],
            "underlyingSymbol": [],
            "position": [],
            "costBasis": [],
            "markPrice": [],
            "putCall": [],
            "strike": [],
            "expiry": [],
            "currency": [],
        }
        positions_df = pd.DataFrame(positions_data)

        # Create mock DataFrame for trades
        trades_data = {
            "symbol": [],
            "underlyingSymbol": [],
            "quantity": [],
            "price": [],
            "dateTime": [],
            "tradeID": [],
            "putCall": [],
            "strike": [],
            "expiry": [],
            "currency": [],
        }
        trades_df = pd.DataFrame(trades_data)

        # Mock the df method to return our DataFrames
        def mock_df(topic):
            if topic == "Position":
                return positions_df
            elif topic == "TradeConfirm":
                return trades_df
            return None

        mock_report_instance.df = Mock(side_effect=mock_df)
        mock_report_instance.topics = Mock(return_value=["Position", "TradeConfirm"])

        # Make the FlexReport class return our mock instance
        mock_report_class.return_value = mock_report_instance

        yield mock_report_class


@pytest.fixture
def mock_flex_report(test_timestamp):
    with patch("sources.flex_client.FlexReport") as mock_report_class:
        # Create a mock for the FlexReport instance
        mock_report_instance = Mock()

        # Mock the download method (not async)
        mock_report_instance.download = Mock()

        # Create mock DataFrame for positions
        positions_data = {
            "symbol": ["AAPL", "AAPL"],
            "underlyingSymbol": ["AAPL", "AAPL"],
            "position": [100, 5],
            "costBasis": [150.25, 3.50],
            "markPrice": [155.50, 4.50],
            "putCall": [None, "C"],
            "strike": [None, 150.00],
            "expiry": [None, "20240615"],
            "currency": ["USD", "USD"],
        }
        positions_df = pd.DataFrame(positions_data)

        # Create mock DataFrame for trades
        trades_data = {
            "symbol": ["AAPL", "AAPL"],
            "underlyingSymbol": ["AAPL", "AAPL"],
            "quantity": [100, 5],
            "price": [150.25, 3.50],
            "dateTime": [
                (test_timestamp - timedelta(minutes=15)).strftime("%Y%m%d;%H%M%S"),
                (test_timestamp - timedelta(minutes=10)).strftime("%Y%m%d;%H%M%S"),
            ],
            "tradeID": ["test-stock-id", "test-option-id"],
            "putCall": [None, "C"],
            "strike": [None, 150.00],
            "expiry": [None, "20240615"],
            "currency": ["USD", "USD"],
        }
        flex_statement_data = {
            "whenGenerated": [(test_timestamp - timedelta(minutes=15)).strftime("%Y%m%d;%H%M%S")],
        }
        trades_df = pd.DataFrame(trades_data)
        flex_statement_df = pd.DataFrame(flex_statement_data)

        # Mock the df method to return our DataFrames
        def mock_df(topic):
            if topic == "Position":
                return positions_df
            elif topic == "TradeConfirm":
                return trades_df
            elif topic == "FlexStatement":
                return flex_statement_df
            return None

        mock_report_instance.df = Mock(side_effect=mock_df)
        mock_report_instance.topics = Mock(
            return_value=["Position", "TradeConfirm", "FlexStatement"]
        )

        # Make the FlexReport class return our mock instance
        mock_report_class.return_value = mock_report_instance

        yield mock_report_class


@pytest.fixture
def mock_flex_report_invalid_trades(test_timestamp):
    with patch("sources.flex_client.FlexReport") as mock_report_class:
        # Create a mock for the FlexReport instance
        mock_report_instance = Mock()

        # Mock the download method (not async)
        mock_report_instance.download = Mock()

        # Create mock DataFrame for positions
        positions_data = {}
        positions_df = pd.DataFrame(positions_data)

        # Create mock DataFrame for trades
        invalid_trades_data = {
            "symbol": ["AAPL"],
            "quantity": ["invalid"],  # Invalid quantity
            "price": [150.25],
            "dateTime": [(test_timestamp - timedelta(minutes=15)).strftime("%Y%m%d;%H%M%S")],
            "tradeID": ["test-trade-id"],
            "putCall": [None],
            "strike": [None],
            "expiry": [None],
            "currency": ["USD"],
        }
        trades_df = pd.DataFrame(invalid_trades_data)

        # Mock the df method to return our DataFrames
        def mock_df(topic):
            if topic == "Position":
                return positions_df
            elif topic == "TradeConfirm":
                return trades_df
            return None

        mock_report_instance.df = Mock(side_effect=mock_df)
        mock_report_instance.topics = Mock(return_value=["Position", "TradeConfirm"])

        # Make the FlexReport class return our mock instance
        mock_report_class.return_value = mock_report_instance

        yield mock_report_class


@pytest.fixture
def mock_flex_report_invalid_options(test_timestamp):
    with patch("sources.flex_client.FlexReport") as mock_report_class:
        # Create a mock for the FlexReport instance
        mock_report_instance = Mock()

        # Mock the download method (not async)
        mock_report_instance.download = Mock()

        # Create mock DataFrame for positions
        positions_data = {}
        positions_df = pd.DataFrame(positions_data)

        # Create DataFrame with invalid option data
        invalid_option_data = {
            "symbol": ["AAPL"],
            "quantity": [5],
            "price": [3.50],
            "dateTime": [(test_timestamp - timedelta(minutes=15)).strftime("%Y%m%d;%H%M%S")],
            "tradeID": ["test-option-id"],
            "putCall": ["C"],
            "strike": ["invalid"],  # Invalid strike price
            "expiry": ["20240615"],
            "currency": ["USD"],
        }
        trades_df = pd.DataFrame(invalid_option_data)

        # Mock the df method to return our DataFrames
        def mock_df(topic):
            if topic == "Position":
                return positions_df
            elif topic == "TradeConfirm":
                return trades_df
            return None

        mock_report_instance.df = Mock(side_effect=mock_df)
        mock_report_instance.topics = Mock(return_value=["Position", "TradeConfirm"])

        # Make the FlexReport class return our mock instance
        mock_report_class.return_value = mock_report_instance

        yield mock_report_class


@pytest.mark.asyncio
async def test_ibkr_source_load_positions(mock_flex_report):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )
    success, _when_generated = await source.load_positions()
    assert success


@pytest.mark.asyncio
async def test_ibkr_source_get_stock_trades(mock_flex_report, test_timestamp):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    success, _when_generated = await source.load_last_day_trades()
    assert success
    trades = source.get_last_day_trades()

    assert len(trades) == 2  # One stock trade and one option trade

    # Verify stock trade
    stock_trade = [t for t in trades if t.instrument.type == InstrumentType.STOCK][0]
    assert stock_trade.instrument.symbol == "AAPL"
    assert stock_trade.quantity == Decimal("100")
    assert stock_trade.price == Decimal("150.25")
    assert stock_trade.instrument.type == InstrumentType.STOCK


@pytest.mark.asyncio
async def test_ibkr_source_get_option_trades(mock_flex_report, test_timestamp):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    await source.load_last_day_trades()
    trades = source.get_last_day_trades()

    assert len(trades) > 0

    # Verify option trade
    option_trade = [t for t in trades if t.instrument.type == InstrumentType.OPTION][0]
    assert option_trade.instrument.symbol == "AAPL"
    assert option_trade.quantity == Decimal("5")
    assert option_trade.price == Decimal("3.50")
    assert option_trade.instrument.type == InstrumentType.OPTION
    assert option_trade.instrument.option_details is not None
    assert option_trade.instrument.option_details.strike == Decimal("150.00")
    assert option_trade.instrument.option_details.option_type == OptionType.CALL
    assert option_trade.instrument.option_details.expiry == date(2024, 6, 15)


@pytest.mark.asyncio
async def test_ibkr_source_empty_trades(mock_empty_flex_report, test_timestamp):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    trades = source.get_last_day_trades()
    assert len(trades) == 0


@pytest.mark.asyncio
async def test_ibkr_source_invalid_trade_data(mock_flex_report_invalid_trades, test_timestamp):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    trades = source.get_last_day_trades()
    assert len(trades) == 0  # Invalid trades should be skipped


@pytest.mark.asyncio
async def test_ibkr_source_invalid_option_data(mock_flex_report_invalid_options, test_timestamp):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    trades = source.get_last_day_trades()
    assert len(trades) == 0  # Invalid option trades should be skipped
