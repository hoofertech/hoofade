import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict

from models.instrument import Instrument, InstrumentType
from utils.datetime_utils import format_date, format_datetime, parse_datetime

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """Represents a position in a financial instrument"""

    instrument: Instrument
    quantity: Decimal  # Positive for long positions, negative for short positions
    cost_basis: Decimal  # Average cost per unit
    market_price: Decimal  # Current market price
    report_time: datetime

    def __post_init__(self):
        """Validate position data after initialization"""
        if self.instrument.type == InstrumentType.OPTION and not self.instrument.option_details:
            raise ValueError("Option positions must include option details")

    @property
    def market_value(self) -> Decimal:
        """Calculate current market value of position"""
        return self.quantity * self.market_price

    @property
    def cost_basis_value(self) -> Decimal:
        """Calculate total cost basis of position"""
        return self.quantity * self.cost_basis

    @property
    def unrealized_pnl(self) -> Decimal:
        """Calculate unrealized P&L in absolute terms"""
        return self.market_value - self.cost_basis_value

    @property
    def unrealized_pnl_percent(self) -> Decimal:
        """Calculate unrealized P&L as a percentage"""
        if self.cost_basis_value == 0:
            return Decimal("0")
        return (self.unrealized_pnl / abs(self.cost_basis_value)) * 100

    @property
    def is_short(self) -> bool:
        """Check if this is a short position"""
        return self.quantity < 0

    @property
    def description(self) -> str:
        """Get a human-readable description of the position"""
        if self.instrument.type == InstrumentType.STOCK:
            return f"{self.quantity:,.0f} {self.instrument.symbol}"

        # Check if option_details exists before accessing its attributes
        if self.instrument.option_details is None:
            return f"{self.quantity:,.0f} {self.instrument.symbol} UNKNOWN OPTION"

        opt = self.instrument.option_details
        direction = "long" if not self.is_short else "short"
        return (
            f"{abs(self.quantity):,.0f} {direction} {self.instrument.symbol} "
            f"{opt.option_type.value} {opt.strike:,.2f} {opt.expiry:%Y-%m-%d}"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Position":
        return cls(
            instrument=Instrument.from_dict(data),
            quantity=Decimal(data["quantity"]),
            cost_basis=Decimal(data["cost_basis"]),
            market_price=Decimal(data["market_price"]),
            report_time=parse_datetime(data["report_time"]),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary for serialization"""
        base_dict = {
            **self.instrument.to_dict(),
            "quantity": str(self.quantity),
            "cost_basis": str(self.cost_basis),
            "market_price": str(self.market_price),
            "report_time": format_datetime(self.report_time),
        }

        # Only add option details if they exist
        if (
            self.instrument.type == InstrumentType.OPTION
            and self.instrument.option_details is not None
        ):
            opt = self.instrument.option_details
            expiry = format_date(opt.expiry)
            if expiry:
                base_dict["expiry"] = expiry
            base_dict["strike"] = str(opt.strike)
            base_dict["option_type"] = opt.option_type.value

        return base_dict
