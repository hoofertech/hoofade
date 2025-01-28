from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Trade:
    symbol: str
    quantity: Decimal
    price: Decimal
    side: str
    timestamp: datetime
    source_id: str
    trade_id: str
