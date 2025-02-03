from formatters.trade import TradeFormatter
import pytest
from models.trade import Trade
from decimal import Decimal


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

    # Process trade from source
    trades = [trade for trade in mock_source.get_last_day_trades()]
    assert len(trades) == 1

    # Format trade
    message = formatter._format_trade(trades[0])

    # Publish to sink
    assert await mock_sink.publish(message)
    assert len(mock_sink.messages) == 1

    # Verify message content
    published_message = mock_sink.messages[0]
    assert "$AAPL" in published_message.content
    assert "15JUN24" in published_message.content
    assert "$150C" in published_message.content
    assert "666" in published_message.content
    assert "$3.50" in published_message.content
