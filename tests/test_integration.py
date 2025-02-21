import pytest

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

    # Process trade from source
    success, _ = await mock_source.load_last_day_trades()
    assert success
    trades = mock_source.get_last_day_trades()
    assert len(trades) == 1

    # Publish to sink
    assert await mock_sink.publish_trades(trades, test_timestamp)
    assert len(mock_sink.published_trades) == 1

    # Verify trade content
    published_trades = mock_sink.published_trades[0]
    assert len(published_trades) == 1
    published_trade = published_trades[0]
    assert published_trade.instrument.symbol == "AAPL"
    assert published_trade.quantity == 666
    assert float(published_trade.price) == 3.50
