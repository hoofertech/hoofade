from datetime import timedelta

import pytest

from services.position_service import PositionService


@pytest.fixture
def position_service(mock_source, mock_sink, db_session):
    sources = {"test": mock_source}
    sinks = {"test": mock_sink}
    return PositionService(sources, sinks, db_session)


@pytest.mark.asyncio
async def test_publish_portfolio(
    position_service, mock_source, mock_sink, sample_positions, test_timestamp
):
    mock_source.positions = sample_positions
    await position_service.publish_portfolio(
        mock_source.get_positions(), test_timestamp, test_timestamp
    )

    assert len(mock_sink.published_portfolios) == 1
    published_positions = mock_sink.published_portfolios[0]
    assert mock_sink.last_portfolio_timestamp == test_timestamp

    # Verify positions match
    assert len(published_positions) == len(sample_positions)
    for actual, expected in zip(published_positions, sample_positions):
        assert actual.instrument.symbol == expected.instrument.symbol
        assert actual.quantity == expected.quantity


@pytest.mark.asyncio
async def test_should_post_portfolio(position_service, mock_source, test_timestamp):
    # Should post if never posted
    assert await position_service.should_post_portfolio(test_timestamp) is True
    await position_service.publish_portfolio(
        mock_source.get_positions(), test_timestamp, test_timestamp
    )

    # Should not post if already posted today
    an_hour_later = test_timestamp + timedelta(hours=1)
    assert await position_service.should_post_portfolio(an_hour_later) is False
    await position_service.publish_portfolio(
        mock_source.get_positions(), an_hour_later, an_hour_later
    )

    # Should post if a day has passed
    tomorrow = test_timestamp + timedelta(days=1)
    assert await position_service.should_post_portfolio(tomorrow) is True
