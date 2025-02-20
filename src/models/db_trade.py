from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import cast

from models.instrument import Instrument, InstrumentType, OptionType
from models.trade import Trade
from utils.datetime_utils import format_date, format_datetime


@dataclass
class DBTrade:
    __tablename__ = "trades"

    trade_id: str
    symbol: str
    instrument_type: str
    quantity: Decimal
    price: Decimal
    side: str
    currency: str
    timestamp: datetime
    source_id: str | None = None

    # Option-specific fields
    option_type: str | None = None
    strike: Decimal | None = None
    expiry: date | None = None

    def to_domain(self) -> Trade:
        # Change the comparison to use string representation for instrument type
        if str(self.instrument_type) == str(InstrumentType.STOCK):
            instrument = Instrument.stock(
                symbol=cast(str, self.symbol), currency=cast(str, self.currency)
            )
        else:
            if not all(
                x is not None
                for x in [
                    getattr(self, "option_type"),
                    getattr(self, "strike"),
                    getattr(self, "expiry"),
                ]
            ):
                raise ValueError("Missing option details for option trade")

            instrument = Instrument.option(
                symbol=cast(str, self.symbol),
                strike=Decimal(str(self.strike)),
                expiry=cast(date, self.expiry),
                option_type=cast(OptionType, self.option_type),
                currency=cast(str, self.currency),
            )

        return Trade(
            trade_id=cast(str, self.trade_id),
            instrument=instrument,
            quantity=Decimal(str(self.quantity)),
            price=Decimal(str(self.price)),
            side=cast(str, self.side),
            timestamp=cast(datetime, self.timestamp),
            source_id=cast(str, self.source_id),
            currency=cast(str, self.currency),
        )

    @classmethod
    def from_domain(cls, trade: Trade) -> "DBTrade":
        db_trade = cls(
            trade_id=trade.trade_id,
            symbol=trade.instrument.symbol,
            instrument_type=trade.instrument.type.value,
            quantity=trade.quantity,
            price=trade.price,
            side=trade.side,
            currency=trade.currency,
            timestamp=trade.timestamp,
            source_id=trade.source_id,
        )

        if trade.instrument.type == InstrumentType.OPTION:
            if not trade.instrument.option_details:
                raise ValueError("Missing option details for option trade")

            # Use setattr to avoid type checking issues with SQLAlchemy columns
            setattr(
                db_trade,
                "option_type",
                trade.instrument.option_details.option_type.value,
            )
            setattr(db_trade, "strike", trade.instrument.option_details.strike)
            setattr(db_trade, "expiry", trade.instrument.option_details.expiry)

        return db_trade

    def to_dict(self):
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "instrument_type": self.instrument_type,
            "quantity": str(self.quantity),
            "price": str(self.price),
            "side": self.side,
            "currency": self.currency,
            "timestamp": format_datetime(self.timestamp),
            "source_id": self.source_id,
            "option_type": self.option_type,
            "strike": str(self.strike) if self.strike is not None else None,
            "expiry": format_date(self.expiry),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DBTrade":
        return cls(**data)
