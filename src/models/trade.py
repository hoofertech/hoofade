from sqlalchemy import String, Numeric, DateTime, Boolean
from sqlalchemy.orm import declarative_base, mapped_column, Mapped
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass


@dataclass
class Trade:
    symbol: str
    quantity: Decimal
    price: Decimal
    side: str  # BUY or SELL
    timestamp: datetime
    source_id: str  # e.g., "ibkr-account1"
    trade_id: str  # unique ID from source
    matched: bool = False


Base = declarative_base()


class DBTrade(Base):
    __tablename__ = "trades"

    trade_id: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)
    matched: Mapped[bool] = mapped_column(Boolean, default=False)

    def to_domain(self) -> Trade:
        """Convert database model to domain model"""
        return Trade(
            symbol=str(self.symbol),
            quantity=Decimal(str(self.quantity)),
            price=Decimal(str(self.price)),
            side=str(self.side),
            timestamp=self.timestamp,
            source_id=str(self.source_id),
            trade_id=str(self.trade_id),
            matched=bool(self.matched),
        )

    @classmethod
    def from_domain(cls, trade: Trade) -> "DBTrade":
        """Create database model from domain model"""
        return cls(
            symbol=str(trade.symbol),
            quantity=str(trade.quantity),
            price=str(trade.price),
            side=str(trade.side),
            timestamp=trade.timestamp,
            source_id=str(trade.source_id),
            trade_id=str(trade.trade_id),
            matched=bool(trade.matched),
        )
