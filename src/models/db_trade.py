from sqlalchemy import Column, String, Numeric, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from models.trade import Trade
from decimal import Decimal
from datetime import datetime
from typing import cast

Base = declarative_base()


class DBTrade(Base):
    __tablename__ = "trades"

    trade_id: Column[str] = Column(String, primary_key=True)
    symbol: Column[str] = Column(String, nullable=False)
    quantity: Column[Decimal] = Column(Numeric, nullable=False)
    price: Column[Decimal] = Column(Numeric, nullable=False)
    side: Column[str] = Column(String, nullable=False)
    timestamp: Column[datetime] = Column(DateTime, nullable=False)
    source_id: Column[str] = Column(String, nullable=False)
    matched: Column[bool] = Column(Boolean, default=False)

    def to_domain(self) -> Trade:
        return Trade(
            trade_id=cast(str, self.trade_id),
            symbol=cast(str, self.symbol),
            quantity=Decimal(str(self.quantity)),
            price=Decimal(str(self.price)),
            side=cast(str, self.side),
            timestamp=cast(datetime, self.timestamp),
            source_id=cast(str, self.source_id),
        )

    @classmethod
    def from_domain(cls, trade: Trade) -> "DBTrade":
        return cls(
            trade_id=trade.trade_id,
            symbol=trade.symbol,
            quantity=trade.quantity,
            price=trade.price,
            side=trade.side,
            timestamp=trade.timestamp,
            source_id=trade.source_id,
        )
