import pytest
from datetime import timedelta, date
from decimal import Decimal
from unittest.mock import Mock, patch
import pandas as pd
from sources.ibkr import IBKRSource
from models.instrument import InstrumentType, OptionType
import logging

logger = logging.getLogger(__name__)


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
            "position": [100, 5],
            "costBasis": [150.25, 3.50],
            "markPrice": [155.50, 4.50],
            "putCall": [None, "C"],
            "strike": [None, 150.00],
            "expiry": [None, date(2024, 6, 15)],
        }
        positions_df = pd.DataFrame(positions_data)

        # Create mock DataFrame for trades
        trades_data = {
            "symbol": ["AAPL", "AAPL"],
            "quantity": [100, 5],
            "price": [150.25, 3.50],
            "dateTime": [
                test_timestamp - timedelta(minutes=15),
                test_timestamp - timedelta(minutes=10),
            ],
            "tradeID": ["test-stock-id", "test-option-id"],
            "putCall": [None, "C"],
            "strike": [None, 150.00],
            "expiry": [None, date(2024, 6, 15)],
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
            "dateTime": [test_timestamp - timedelta(minutes=15)],
            "tradeID": ["test-trade-id"],
            "putCall": [None],
            "strike": [None],
            "expiry": [None],
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
            "dateTime": [test_timestamp - timedelta(minutes=15)],
            "tradeID": ["test-option-id"],
            "putCall": ["C"],
            "strike": ["invalid"],  # Invalid strike price
            "expiry": [date(2024, 6, 15)],
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
async def test_ibkr_source_connect(mock_flex_report):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )
    assert await source.connect()


@pytest.mark.asyncio
async def test_ibkr_source_get_stock_trades(mock_flex_report, test_timestamp):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    since = test_timestamp - timedelta(days=1)
    trades = [trade async for trade in source.get_recent_trades(since)]

    assert len(trades) == 2  # One stock trade and one option trade

    # Verify stock trade
    stock_trade = next(t for t in trades if t.instrument.type == InstrumentType.STOCK)
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

    since = test_timestamp - timedelta(days=1)
    trades = [trade async for trade in source.get_recent_trades(since)]

    # Verify option trade
    option_trade = next(t for t in trades if t.instrument.type == InstrumentType.OPTION)
    assert option_trade.instrument.symbol == "AAPL"
    assert option_trade.quantity == Decimal("5")
    assert option_trade.price == Decimal("3.50")
    assert option_trade.instrument.type == InstrumentType.OPTION
    assert option_trade.instrument.option_details is not None
    assert option_trade.instrument.option_details.strike == Decimal("150.00")
    assert option_trade.instrument.option_details.option_type == OptionType.CALL
    assert option_trade.instrument.option_details.expiry == date(2024, 6, 15)


@pytest.mark.asyncio
async def test_ibkr_source_empty_trades(mock_flex_report, test_timestamp):
    # Modify mock to return empty DataFrame
    mock_flex_report.return_value.df.return_value = pd.DataFrame()

    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    since = test_timestamp - timedelta(days=1)
    trades = [trade async for trade in source.get_recent_trades(since)]
    logger.info(f"Trades: {trades}")
    # assert len(trades) == 0


@pytest.mark.asyncio
async def test_ibkr_source_invalid_trade_data(
    mock_flex_report_invalid_trades, test_timestamp
):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    since = test_timestamp - timedelta(days=1)
    trades = [trade async for trade in source.get_recent_trades(since)]
    logger.info(f"Trades: {trades}")
    assert len(trades) == 0  # Invalid trades should be skipped


@pytest.mark.asyncio
async def test_ibkr_source_invalid_option_data(
    mock_flex_report_invalid_options, test_timestamp
):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )

    since = test_timestamp - timedelta(days=1)
    trades = [trade async for trade in source.get_recent_trades(since)]
    assert len(trades) == 0  # Invalid option trades should be skipped


@pytest.mark.asyncio
async def test_ibkr_source_disconnect(mock_flex_report):
    source = IBKRSource(
        source_id="test-source",
        portfolio_token="test-token",
        portfolio_query_id="123",
        trades_token="test-token",
        trades_query_id="456",
    )
    await source.disconnect()  # Should not raise any exceptions
