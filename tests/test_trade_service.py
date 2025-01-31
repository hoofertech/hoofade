import pytest
from datetime import timedelta
from decimal import Decimal
from services.trade_service import TradeService
from models.trade import Trade
from formatters.trade import TradeFormatter


@pytest.fixture
def trade_service(mock_source, mock_sink, db_session):
    sources = {"test": mock_source}
    sinks = {"test": mock_sink}
    return TradeService(sources, sinks, db_session, formatter=TradeFormatter())


@pytest.mark.asyncio
async def test_get_new_trades(trade_service, sample_trade, db_session):
    # First time should return the trade
    new_trades = await trade_service.get_new_trades()
    assert len(new_trades) == 1
    assert new_trades[0][0].trade_id == sample_trade.trade_id
    assert new_trades[0][1] is None  # No matching trade

    # Second time should return empty (already published)
    new_trades = await trade_service.get_new_trades()
    assert len(new_trades) == 0


@pytest.mark.asyncio
async def test_get_new_trades_with_matching(
    trade_service, sample_trade, matching_trade
):
    # Add both trades
    trade_service.sources["test"].trades = [sample_trade, matching_trade]

    new_trades = await trade_service.get_new_trades()
    assert len(new_trades) == 2

    # Verify matching trade is linked
    closing_trade = new_trades[1]
    assert closing_trade[0].trade_id == matching_trade.trade_id
    assert closing_trade[1].trade_id == sample_trade.trade_id


@pytest.mark.asyncio
async def test_publish_trades(trade_service, mock_sink, sample_trade):
    trades = [(sample_trade, None)]
    await trade_service.publish_trades(trades)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    # Verify content structure
    lines = message.content.split("\n")
    expected_date = sample_trade.timestamp.strftime("%d %b %Y %H:%M").upper()
    assert lines[0] == f"Trades on {expected_date}"
    assert lines[1] == ""
    assert "🚨 Buy  $AAPL 100@$150.25" in lines[2]
    assert message.metadata["type"] == "trade_batch"


@pytest.mark.asyncio
async def test_publish_multiple_trades(trade_service, mock_sink, sample_trade):
    # Create second trade
    trade2 = Trade(
        instrument=sample_trade.instrument,
        quantity=Decimal("200"),
        price=Decimal("151.25"),
        side="BUY",
        timestamp=sample_trade.timestamp + timedelta(minutes=30),
        source_id="test-source",
        trade_id="test-exec-id-2",
        currency="USD",
    )

    trades = [(sample_trade, None), (trade2, None)]
    await trade_service.publish_trades(trades)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    lines = message.content.split("\n")
    expected_date = trade2.timestamp.strftime("%d %b %Y %H:%M").upper()
    assert lines[0] == f"Trades on {expected_date}"
    assert lines[1] == ""
    assert "🚨 Buy  $AAPL 100@$150.25" in lines[2]
    assert "🚨 Buy  $AAPL 200@$151.25" in lines[3]


@pytest.mark.asyncio
async def test_empty_trades_no_publish(trade_service, mock_sink):
    await trade_service.publish_trades([])
    assert len(mock_sink.messages) == 0
