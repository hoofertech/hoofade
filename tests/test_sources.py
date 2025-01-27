import pytest
from datetime import timedelta
from src.sources.ibkr import IBKRSource
from unittest.mock import Mock, patch


@pytest.mark.asyncio
async def test_ibkr_source_connection():
    with patch("ib_insync.IB") as mock_ib:
        mock_ib_instance = Mock()
        mock_ib_instance.connect.return_value = None
        mock_ib_instance.isConnected.return_value = True
        mock_ib.return_value = mock_ib_instance

        source = IBKRSource("test-ibkr", "localhost", 7496, 1)
        source.ib = mock_ib_instance
        assert source.connect() is True


@pytest.mark.asyncio
async def test_ibkr_source_get_recent_trades(test_timestamp):
    with patch("ib_insync.IB") as mock_ib:
        mock_ib_instance = Mock()
        mock_ib_instance.isConnected.return_value = True

        mock_trade = Mock()
        mock_trade.time = test_timestamp
        mock_trade.contract.symbol = "AAPL"
        mock_trade.execution.shares = 100
        mock_trade.execution.price = 150.25
        mock_trade.execution.side = "BOT"
        mock_trade.execution.execId = "test-exec-id"

        mock_ib_instance.trades.return_value = [mock_trade]
        mock_ib.return_value = mock_ib_instance

        source = IBKRSource("test-ibkr", "localhost", 7496, 1)
        source.ib = mock_ib_instance

        since_time = test_timestamp - timedelta(minutes=15)
        trades = list(source.get_recent_trades(since_time))
        assert len(trades) == 1
        assert trades[0].symbol == "AAPL"
