from sqlalchemy import Column, String, Numeric, DateTime, Boolean, Date, Enum
from sqlalchemy.orm import declarative_base
from models.trade import Trade
from models.instrument import Instrument, InstrumentType, OptionType
from decimal import Decimal
from datetime import datetime, date
from typing import cast

Base = declarative_base()


class DBTrade(Base):
    __tablename__ = "trades"

    trade_id: Column[str] = Column(String, primary_key=True)
    symbol: Column[str] = Column(String, nullable=False)
    instrument_type: Column[str] = Column(
        Enum(InstrumentType, name="instrument_type"), nullable=False
    )
    quantity: Column[Decimal] = Column(Numeric, nullable=False)
    price: Column[Decimal] = Column(Numeric, nullable=False)
    side: Column[str] = Column(String, nullable=False)
    currency: Column[str] = Column(String, nullable=False)
    timestamp: Column[datetime] = Column(DateTime, nullable=False)
    source_id: Column[str] = Column(String, nullable=False)
    matched: Column[bool] = Column(Boolean, default=False)

    # Option-specific fields
    option_type: Column[str] = Column(
        Enum(OptionType, name="option_type"), nullable=True
    )
    strike: Column[Decimal] = Column(Numeric, nullable=True)
    expiry: Column[date] = Column(Date, nullable=True)

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
            instrument_type=trade.instrument.type,
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
                db_trade, "option_type", trade.instrument.option_details.option_type
            )
            setattr(db_trade, "strike", trade.instrument.option_details.strike)
            setattr(db_trade, "expiry", trade.instrument.option_details.expiry)

        return db_trade
