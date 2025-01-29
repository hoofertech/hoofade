import pytest
from decimal import Decimal
from models.position import Position
from models.instrument import Instrument, InstrumentType, OptionDetails, OptionType
from datetime import date


@pytest.fixture
def stock_position():
    instrument = Instrument(symbol="AAPL", type=InstrumentType.STOCK)
    return Position(
        instrument=instrument,
        quantity=Decimal("100"),
        cost_basis=Decimal("150.00"),
        market_price=Decimal("160.00"),
    )


@pytest.fixture
def long_call_position():
    instrument = Instrument(
        symbol="AAPL",
        type=InstrumentType.OPTION,
        option_details=OptionDetails(
            strike=Decimal("165.00"),
            expiry=date(2024, 12, 20),
            option_type=OptionType.CALL,
        ),
    )
    return Position(
        instrument=instrument,
        quantity=Decimal("2"),
        cost_basis=Decimal("3.50"),
        market_price=Decimal("4.20"),
    )


@pytest.fixture
def short_put_position():
    instrument = Instrument(
        symbol="AAPL",
        type=InstrumentType.OPTION,
        option_details=OptionDetails(
            strike=Decimal("145.00"),
            expiry=date(2024, 12, 20),
            option_type=OptionType.PUT,
        ),
    )
    return Position(
        instrument=instrument,
        quantity=Decimal("-3"),
        cost_basis=Decimal("2.50"),
        market_price=Decimal("2.10"),
    )


def test_stock_position_description(stock_position):
    assert stock_position.description == "100 AAPL"


def test_long_call_position_description(long_call_position):
    assert long_call_position.description == "2 long AAPL call 165.00 2024-12-20"


def test_short_put_position_description(short_put_position):
    assert short_put_position.description == "3 short AAPL put 145.00 2024-12-20"


def test_invalid_option_position():
    with pytest.raises(ValueError):
        Position(
            instrument=Instrument(
                symbol="AAPL",
                type=InstrumentType.OPTION,  # Missing option_details
            ),
            quantity=Decimal("1"),
            cost_basis=Decimal("1.00"),
            market_price=Decimal("1.00"),
        )


def test_stock_position_to_dict(stock_position):
    position_dict = stock_position.to_dict()
    assert position_dict["symbol"] == "AAPL"
    assert position_dict["type"] == "stock"
    assert position_dict["quantity"] == "100"
    assert position_dict["market_value"] == "16000.00"
    assert "strike" not in position_dict


def test_option_position_to_dict(long_call_position):
    position_dict = long_call_position.to_dict()
    assert position_dict["symbol"] == "AAPL"
    assert position_dict["type"] == "option"
    assert position_dict["strike"] == "165.00"
    assert position_dict["option_type"] == "call"
    assert position_dict["expiry"] == "2024-12-20"


def test_position_pnl_calculations(short_put_position):
    assert short_put_position.market_value == Decimal("-6.30")
    assert short_put_position.cost_value == Decimal("-7.50")
    assert short_put_position.unrealized_pnl == Decimal("1.20")
    assert short_put_position.unrealized_pnl_percent == Decimal("16.00")
