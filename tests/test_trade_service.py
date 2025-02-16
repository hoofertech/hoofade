import logging
from datetime import timedelta
from decimal import Decimal

import pytest

from formatters.trade import TradeFormatter
from models.trade import Trade
from services.position_service import PositionService
from services.trade_processor import ProfitTaker
from services.trade_service import TradeService

logger = logging.getLogger(__name__)


@pytest.fixture
async def trade_service(mock_source, mock_sink, db_session):
    sources = {"test": mock_source}
    sinks = {"test": mock_sink}
    await mock_source.load_last_day_trades()
    return TradeService(
        sources,
        sinks,
        db_session,
        formatter=TradeFormatter(),
        position_service=PositionService(sources, sinks, db_session),
    )


@pytest.mark.asyncio
async def test_get_new_trades(trade_service, sample_trade, db_session):
    # First time should return the trade
    new_trades, _portfolio_matches = await trade_service.get_new_trades()
    assert len(new_trades) == 1
    assert len(new_trades[0].trades) == 1
    assert new_trades[0].trades[0].trade_id == sample_trade.trade_id

    # Second time should return empty (already published)
    new_trades, _portfolio_matches = await trade_service.get_new_trades()
    assert len(new_trades) == 0


@pytest.mark.asyncio
async def test_get_new_trades_with_matching(trade_service, sample_trade, matching_trade):
    # Add both trades
    trade_service.sources["test"].last_day_trades = [sample_trade, matching_trade]

    new_trades, _portfolio_matches = await trade_service.get_new_trades()
    assert len(new_trades) == 1
    profit_taker = new_trades[0]
    assert isinstance(profit_taker, ProfitTaker)

    assert profit_taker.sell_trade.trades[0].trade_id == matching_trade.trade_id
    assert profit_taker.buy_trade.trades[0].trade_id == sample_trade.trade_id


@pytest.mark.asyncio
async def test_publish_trades(trade_service, mock_sink, sample_trade):
    trades = [sample_trade]
    await trade_service.publish_trades(trades)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    # Verify content structure
    lines = message.content.split("\n")
    expected_date = sample_trade.timestamp.strftime("%d %b %Y %H:%M").upper()
    assert lines[0] == f"Trades on {expected_date}"
    assert lines[1] == ""
    assert "🚨 BUY  $AAPL 100@$150.25" in lines[2]
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

    trades = [sample_trade, trade2]
    await trade_service.publish_trades(trades)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    lines = message.content.split("\n")
    expected_date = trade2.timestamp.strftime("%d %b %Y %H:%M").upper()
    assert lines[0] == f"Trades on {expected_date}"
    assert lines[1] == ""
    assert "🚨 BUY  $AAPL 100@$150.25" in lines[2]
    assert "🚨 BUY  $AAPL 200@$151.25" in lines[3]


@pytest.mark.asyncio
async def test_empty_trades_no_publish(trade_service, mock_sink):
    await trade_service.publish_trades([])
    assert len(mock_sink.messages) == 0
