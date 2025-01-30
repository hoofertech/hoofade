import pytest
from pathlib import Path
from decimal import Decimal
from sources.ibkr_json_source import JsonSource
from models.instrument import InstrumentType, OptionType
from datetime import date


@pytest.fixture
def test_data_dir():
    return Path("tests/test_data")


@pytest.fixture
async def json_source(test_data_dir):
    source = JsonSource(source_id="test-source", data_dir=str(test_data_dir))
    await source.connect()
    yield source
    await source.disconnect()


@pytest.mark.asyncio
async def test_json_source_connect(test_data_dir):
    source = JsonSource(source_id="test-source", data_dir=str(test_data_dir))
    assert await source.connect()


@pytest.mark.asyncio
async def test_json_source_get_positions(json_source):
    positions = await json_source.get_positions()
    assert len(positions) > 0

    # Test stock position
    baba_position = next(p for p in positions if p.instrument.symbol == "BABA")
    assert baba_position.instrument.type == InstrumentType.STOCK
    assert baba_position.quantity == Decimal("600")
    assert baba_position.cost_basis == Decimal("82.158479842")
    assert baba_position.market_price == Decimal("96.03")

    # Test option position
    nvda_put = next(
        p
        for p in positions
        if p.instrument.symbol == "NVDA" and p.instrument.option_details is not None
    )
    assert nvda_put.instrument.type == InstrumentType.OPTION
    assert nvda_put.quantity == Decimal("1")
    assert nvda_put.instrument.option_details.strike == Decimal("100")
    assert nvda_put.instrument.option_details.option_type == OptionType.PUT
    assert nvda_put.instrument.option_details.expiry == date(2025, 7, 18)


@pytest.mark.asyncio
async def test_json_source_negative_positions(json_source):
    positions = await json_source.get_positions()

    # Test negative stock position (short)
    pltr_position = next(p for p in positions if p.instrument.symbol == "PLTR")
    assert pltr_position.quantity == Decimal("-200")
    assert pltr_position.cost_basis == Decimal("76.59264118")
    assert pltr_position.market_price == Decimal("80.23")

    # Test negative option position (short)
    amd_put = next(
        p
        for p in positions
        if p.instrument.symbol == "AMD" and p.instrument.option_details is not None
    )
    assert amd_put.quantity == Decimal("-1")
    assert amd_put.instrument.option_details.strike == Decimal("106")
    assert amd_put.instrument.option_details.option_type == OptionType.PUT
    assert amd_put.instrument.option_details.expiry == date(2025, 1, 31)


@pytest.mark.asyncio
async def test_json_source_disconnect(json_source):
    await json_source.disconnect()
    assert len(json_source.positions) == 0
