from datetime import timedelta
from formatters.trade import TradeFormatter
import pytest


@pytest.mark.asyncio
async def test_end_to_end_flow(test_timestamp, mock_source, mock_sink):
    # Setup
    formatter = TradeFormatter()
    since = test_timestamp - timedelta(minutes=15)

    # Process trade from source
    trades = [trade async for trade in mock_source.get_recent_trades(since)]
    assert len(trades) == 1

    # Format trade
    message = formatter.format_trade(trades[0])

    # Publish to sink
    assert await mock_sink.publish(message)
    assert len(mock_sink.messages) == 1

    # Verify message content
    published_message = mock_sink.messages[0]
    assert "$AAPL" in published_message.content
    assert "100 shares" in published_message.content
    assert "$150.25" in published_message.content


@pytest.mark.asyncio
async def test_end_to_end_flow_with_matching_trade(
    test_timestamp, mock_source, mock_sink, matching_trade
):
    # Add matching trade to source
    mock_source.trades.append(matching_trade)

    # Setup
    formatter = TradeFormatter()
    since = test_timestamp - timedelta(minutes=15)

    # Process trades from source
    trades = [trade async for trade in mock_source.get_recent_trades(since)]
    assert len(trades) == 2

    # Format and publish second trade (which closes the position)
    message = formatter.format_trade(trades[1], trades[0])

    # Publish to sink
    assert await mock_sink.publish(message)
    assert len(mock_sink.messages) == 1

    # Verify message content
    published_message = mock_sink.messages[0]
    assert "$AAPL" in published_message.content
    assert "P&L: -6.66%" in published_message.content
    assert "Hold time: 2 hours 30 minutes" in published_message.content
