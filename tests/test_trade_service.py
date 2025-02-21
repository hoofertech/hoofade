import logging
from datetime import timedelta
from decimal import Decimal

import pytest

from formatters.trade import TradeFormatter
from models.trade import Trade
from services.position_service import PositionService
from services.trade_processor import ProfitTaker, TradeProcessor
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
async def test_get_new_trades(trade_service, mock_source, sample_trade):
    mock_source.trades = [sample_trade.to_dict()]
    success, _ = await mock_source.load_last_day_trades()
    assert success

    new_trades = await trade_service.get_new_trades()
    assert len(new_trades) == 1
    assert new_trades[0].trade_id == sample_trade.trade_id


@pytest.mark.asyncio
async def test_get_new_trades_with_matching(trade_service, sample_trade, matching_trade):
    # Add both trades
    trade_service.sources["test"].last_day_trades = [sample_trade, matching_trade]

    new_trades = await trade_service.get_new_trades()
    assert len(new_trades) == 2
    trade_processor = TradeProcessor(trade_service.position_service.merged_positions)
    processed_results, _ = trade_processor.process_trades(new_trades)
    assert len(processed_results) == 1
    profit_taker = processed_results[0]
    assert isinstance(profit_taker, ProfitTaker)

    assert profit_taker.sell_trade.trades[0].trade_id == matching_trade.trade_id
    assert profit_taker.buy_trade.trades[0].trade_id == sample_trade.trade_id


@pytest.mark.asyncio
async def test_publish_trades_svc(trade_service, mock_sink, sample_trade, test_timestamp):
    trades = [sample_trade]
    await trade_service.publish_trades_svc(trades, test_timestamp)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    # Verify content structure
    lines = message.content.split("\n")
    expected_date = sample_trade.timestamp.strftime("%d %b %Y %H:%M").upper()
    assert lines[0] == f"Trades on {expected_date}"
    assert lines[1] == ""
    assert "🚨 BUY  $AAPL 100@$150.25" in lines[2]
    assert message.metadata["type"] == "trd"


@pytest.mark.asyncio
async def test_publish_multiple_trades(trade_service, mock_sink, sample_trade, test_timestamp):
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
    await trade_service.publish_trades_svc(trades, test_timestamp)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    lines = message.content.split("\n")
    expected_date = trade2.timestamp.strftime("%d %b %Y %H:%M").upper()
    assert lines[0] == f"Trades on {expected_date}"
    assert lines[1] == ""
    assert "🚨 BUY  $AAPL 300@$150.92" in lines[2]


@pytest.mark.asyncio
async def test_empty_trades_no_publish(trade_service, mock_sink, test_timestamp):
    await trade_service.publish_trades_svc([], test_timestamp)
    assert len(mock_sink.messages) == 0
