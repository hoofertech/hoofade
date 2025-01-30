from datetime import timedelta
from formatters.trade import TradeFormatter
import pytest
from models.trade import Trade
from decimal import Decimal


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
    assert "100" in published_message.content
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
    assert "-6.66%" in published_message.content
    assert "2 hours 30 minutes" in published_message.content


@pytest.mark.asyncio
async def test_end_to_end_flow_with_option_trade(
    test_timestamp, mock_source, mock_sink, call_option_instrument
):
    # Create an option trade
    option_trade = Trade(
        instrument=call_option_instrument,
        quantity=Decimal("666"),
        price=Decimal("3.50"),
        side="BUY",
        timestamp=test_timestamp,
        source_id="test-source",
        trade_id="test-option-exec-1",
        currency="USD",
    )

    mock_source.trades = [option_trade]

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
    assert "15JUN2024" in published_message.content
    assert "$150 C" in published_message.content
    assert "666" in published_message.content
    assert "$3.50" in published_message.content
