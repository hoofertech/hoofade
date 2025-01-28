import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch
import pandas as pd
from sources.ibkr import IBKRSource


@pytest.fixture
def mock_flex_report():
    with patch("sources.flex_client.FlexReport") as mock_report_class:
        # Create a mock for the FlexReport instance
        mock_report_instance = Mock()

        # Mock the download method (not async)
        mock_report_instance.download = Mock()

        # Create mock DataFrame for positions
        positions_data = {
            "symbol": ["AAPL"],
            "position": [100],
            "costBasis": [150.25],
            "markPrice": [155.50],
        }
        positions_df = pd.DataFrame(positions_data)

        # Create mock DataFrame for trades
        trades_data = {
            "symbol": ["AAPL"],
            "quantity": [100],
            "price": [150.25],
            "dateTime": [datetime.now(timezone.utc)],
            "tradeID": ["test-trade-id"],
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

        # Make the FlexReport class return our mock instance
        mock_report_class.return_value = mock_report_instance

        yield mock_report_class


@pytest.mark.asyncio
async def test_ibkr_source_connection(mock_flex_report):
    source = IBKRSource(
        source_id="test-ibkr",
        portfolio_token="test-token",
        portfolio_query_id="test-query",
        trades_token="test-token",
        trades_query_id="test-query",
    )

    assert await source.connect() is True
    mock_flex_report.assert_called_with(token="test-token", queryId="test-query")
    mock_instance = mock_flex_report.return_value
    mock_instance.download.assert_called_with("test-token", "test-query")


@pytest.mark.asyncio
async def test_ibkr_source_connection_failure(mock_flex_report):
    mock_instance = mock_flex_report.return_value
    mock_instance.download.side_effect = Exception("Failed to download report")

    source = IBKRSource(
        source_id="test-ibkr",
        portfolio_token="test-token",
        portfolio_query_id="test-query",
        trades_token="test-token",
        trades_query_id="test-query",
    )

    assert await source.connect() is False


@pytest.mark.asyncio
async def test_ibkr_source_get_recent_trades(mock_flex_report, test_timestamp):
    source = IBKRSource(
        source_id="test-ibkr",
        portfolio_token="test-token",
        portfolio_query_id="test-query",
        trades_token="test-token",
        trades_query_id="test-query",
    )

    since_time = test_timestamp - timedelta(minutes=15)
    trades = [trade async for trade in source.get_recent_trades(since_time)]

    assert len(trades) == 1
    assert trades[0].symbol == "AAPL"
    assert trades[0].quantity == Decimal("100")
    assert trades[0].price == Decimal("150.25")
    assert trades[0].side == "BUY"

    mock_flex_report.assert_called_with(token="test-token", queryId="test-query")
    mock_instance = mock_flex_report.return_value
    mock_instance.download.assert_called_once()


@pytest.mark.asyncio
async def test_ibkr_source_get_recent_trades_empty(mock_flex_report, test_timestamp):
    # Override the mock to return empty DataFrame
    mock_instance = mock_flex_report.return_value
    mock_instance.df = Mock(return_value=pd.DataFrame())

    source = IBKRSource(
        source_id="test-ibkr",
        portfolio_token="test-token",
        portfolio_query_id="test-query",
        trades_token="test-token",
        trades_query_id="test-query",
    )

    since_time = test_timestamp - timedelta(minutes=15)
    trades = [trade async for trade in source.get_recent_trades(since_time)]

    assert len(trades) == 0


@pytest.mark.asyncio
async def test_ibkr_source_get_recent_trades_none_df(mock_flex_report, test_timestamp):
    # Override the mock to return None
    mock_instance = mock_flex_report.return_value
    mock_instance.df = Mock(return_value=None)

    source = IBKRSource(
        source_id="test-ibkr",
        portfolio_token="test-token",
        portfolio_query_id="test-query",
        trades_token="test-token",
        trades_query_id="test-query",
    )

    since_time = test_timestamp - timedelta(minutes=15)
    trades = [trade async for trade in source.get_recent_trades(since_time)]

    assert len(trades) == 0
