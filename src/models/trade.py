from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


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
