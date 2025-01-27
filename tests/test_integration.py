from datetime import timedelta
from src.formatters.trade import TradeFormatter


def test_end_to_end_flow(test_timestamp, mock_source, mock_sink):
    # Setup
    formatter = TradeFormatter()
    since = test_timestamp - timedelta(minutes=15)

    # Process trade from source
    trades = list(mock_source.get_recent_trades(since))
    assert len(trades) == 1

    # Format trade
    message = formatter.format_trade(trades[0])

    # Publish to sink
    assert mock_sink.publish(message)
    assert len(mock_sink.messages) == 1

    # Verify message content
    published_message = mock_sink.messages[0]
    assert "$AAPL" in published_message.content
    assert "100 shares" in published_message.content
    assert "$150.25" in published_message.content
