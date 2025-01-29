from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional


class InstrumentType(Enum):
    STOCK = "stock"
    OPTION = "option"


class OptionType(Enum):
    CALL = "call"
    PUT = "put"


@dataclass
class OptionDetails:
    strike: Decimal
    expiry: date
    option_type: OptionType


@dataclass
class Instrument:
    symbol: str
    type: InstrumentType
    option_details: Optional[OptionDetails] = None

    @classmethod
    def stock(cls, symbol: str) -> "Instrument":
        return cls(symbol=symbol, type=InstrumentType.STOCK)

    @classmethod
    def option(
        cls, symbol: str, strike: Decimal, expiry: date, option_type: OptionType
    ) -> "Instrument":
        return cls(
            symbol=symbol,
            type=InstrumentType.OPTION,
            option_details=OptionDetails(
                strike=strike,
                expiry=expiry,
                option_type=option_type,
            ),
        )
