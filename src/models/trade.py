from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from models.instrument import Instrument


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
