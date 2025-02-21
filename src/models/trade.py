from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict

from models.instrument import Instrument
from utils.datetime_utils import format_datetime, parse_datetime


@dataclass
class Trade:
    instrument: Instrument
    quantity: Decimal
    price: Decimal
    currency: str
    side: str
    timestamp: datetime
    trade_id: str
    source_id: str | None = None

    @classmethod
    def from_dict(cls, data: Dict) -> "Trade":
        return cls(
            instrument=Instrument.from_dict(data),
            quantity=Decimal(data["quantity"]),
            price=Decimal(data["price"]),
            currency=data["currency"],
            side=data["side"],
            timestamp=parse_datetime(data["timestamp"]),
            trade_id=data["trade_id"],
            source_id=data["source_id"],
        )

    def to_dict(self) -> Dict:
        return {
            "ttype": "trade",
            **self.instrument.to_dict(),
            "quantity": str(self.quantity),
            "price": str(self.price),
            "currency": self.currency,
            "side": self.side,
            "timestamp": format_datetime(self.timestamp),
            "trade_id": self.trade_id,
            "source_id": self.source_id,
        }
