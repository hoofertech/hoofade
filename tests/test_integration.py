import pytest

from formatters.trade import TradeFormatter
from models.instrument import OptionType


@pytest.mark.asyncio
async def test_end_to_end_flow_with_option_trade(
    test_timestamp, mock_source, mock_sink, call_option_instrument
):
    # Create an option trade

    option_trade = {
        "accountId": "U7170000",
        "currency": call_option_instrument.currency,
        "symbol": call_option_instrument.symbol,
        "listingExchange": "CBOE",
        "underlyingSymbol": call_option_instrument.symbol,
        "expiry": call_option_instrument.expiry.strftime("%Y%m%d"),
        "putCall": "C" if call_option_instrument.option_type == OptionType.CALL else "P",
        "tradeID": 466929324,
        "dateTime": test_timestamp.strftime("%Y%m%d;%H%M%S"),
        "buySell": "BUY",
        "quantity": 666,
        "price": 3.50,
        "strike": call_option_instrument.strike,
    }

    mock_source.trades = [option_trade]

    # Setup
    formatter = TradeFormatter()

    # Process trade from source
    success, _ = await mock_source.load_last_day_trades()
    assert success
    trades = mock_source.get_last_day_trades()
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
