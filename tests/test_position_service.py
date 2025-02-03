import pytest
from datetime import datetime, timezone, timedelta
from services.position_service import PositionService
from models.position import Position


@pytest.fixture
def position_service(mock_source, mock_sink):
    sources = {"test": mock_source}
    sinks = {"test": mock_sink}
    return PositionService(sources, sinks)


@pytest.mark.asyncio
async def test_get_positions(position_service, mock_source, sample_positions):
    mock_source.positions = sample_positions
    positions = await position_service.get_positions(mock_source)
    assert len(positions) == len(sample_positions)
    assert all(isinstance(p, Position) for p in positions)


@pytest.mark.asyncio
async def test_publish_portfolio(
    position_service, mock_source, mock_sink, sample_positions
):
    mock_source.positions = sample_positions
    await position_service.publish_portfolio(mock_source)

    assert len(mock_sink.messages) == 1
    message = mock_sink.messages[0]

    # Verify content structure
    lines = message.content.split("\n")
    expected_date = datetime.now(timezone.utc).strftime("%d %b %Y").upper()
    assert lines[0] == f"Portfolio on {expected_date}"
    assert message.metadata["type"] == "portfolio"


def test_should_post_portfolio(position_service, test_timestamp):
    # Should post if never posted
    assert position_service.should_post_portfolio(test_timestamp) is True

    # Should not post if already posted today
    position_service.last_portfolio_post = test_timestamp + timedelta(hours=1)
    assert position_service.should_post_portfolio(test_timestamp) is False

    # Should post if last post was yesterday
    position_service.last_portfolio_post = test_timestamp - timedelta(days=1)
    assert position_service.should_post_portfolio(test_timestamp) is True
