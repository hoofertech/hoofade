from datetime import datetime, timedelta
from decimal import Decimal
from src.sources.ibkr import IBKRSource
from unittest.mock import Mock, patch


def test_ibkr_source_connection():
    with patch("ib_insync.IB") as mock_ib:
        source = IBKRSource("test-ibkr", "localhost", 7496, 1)
        mock_ib.return_value.connect.return_value = None

        assert source.connect()
        mock_ib.return_value.connect.assert_called_once_with(
            "localhost", 7496, clientId=1
        )


def test_ibkr_source_get_recent_trades():
    with patch("ib_insync.IB") as mock_ib:
        source = IBKRSource("test-ibkr", "localhost", 7496, 1)

        # Mock trade data
        mock_trade = Mock()
        mock_trade.time = datetime.now()
        mock_trade.contract.symbol = "AAPL"
        mock_trade.execution.shares = 100
        mock_trade.execution.price = 150.25
        mock_trade.execution.side = "BOT"
        mock_trade.execution.execId = "test-exec-id"

        mock_ib.return_value.trades.return_value = [mock_trade]

        # Get recent trades
        trades = list(source.get_recent_trades(datetime.now() - timedelta(minutes=15)))

        assert len(trades) == 1
        assert trades[0].symbol == "AAPL"
        assert trades[0].quantity == Decimal("100")
        assert trades[0].price == Decimal("150.25")
        assert trades[0].side == "BUY"
