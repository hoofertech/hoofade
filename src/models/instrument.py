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
    currency: str
    option_details: Optional[OptionDetails] = None

    @classmethod
    def stock(cls, symbol: str, currency: str) -> "Instrument":
        return cls(symbol=symbol, type=InstrumentType.STOCK, currency=currency)

    @classmethod
    def option(
        cls,
        symbol: str,
        strike: Decimal,
        expiry: date | None,
        option_type: OptionType,
        currency: str,
    ) -> "Instrument":
        if expiry is None:
            raise ValueError("Expiry is required for options")
        return cls(
            symbol=symbol,
            type=InstrumentType.OPTION,
            option_details=OptionDetails(
                strike=strike,
                expiry=expiry,
                option_type=option_type,
            ),
            currency=currency,
        )

    @property
    def strike(self) -> Decimal:
        if self.type != InstrumentType.OPTION:
            raise ValueError("Strike is only defined for options")
        return self.option_details.strike

    @property
    def expiry(self) -> date:
        if self.type != InstrumentType.OPTION:
            raise ValueError("Expiry is only defined for options")
        return self.option_details.expiry

    @property
    def option_type(self) -> OptionType:
        if self.type != InstrumentType.OPTION:
            raise ValueError("Option type is only defined for options")
        return self.option_details.option_type
