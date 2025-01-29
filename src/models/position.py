from dataclasses import dataclass
from decimal import Decimal
from models.instrument import Instrument, InstrumentType


@dataclass
class Position:
    """Represents a position in a financial instrument"""

    instrument: Instrument
    quantity: Decimal  # Positive for long positions, negative for short positions
    cost_basis: Decimal  # Average cost per unit
    market_price: Decimal  # Current market price

    def __post_init__(self):
        """Validate position data after initialization"""
        if (
            self.instrument.type == InstrumentType.OPTION
            and not self.instrument.option_details
        ):
            raise ValueError("Option positions must include option details")

    @property
    def market_value(self) -> Decimal:
        """Calculate total market value of the position"""
        return self.quantity * self.market_price

    @property
    def cost_value(self) -> Decimal:
        """Calculate total cost value of the position"""
        return self.quantity * self.cost_basis

    @property
    def unrealized_pnl(self) -> Decimal:
        """Calculate unrealized profit/loss"""
        return self.market_value - self.cost_value

    @property
    def unrealized_pnl_percent(self) -> Decimal:
        """Calculate unrealized profit/loss as a percentage"""
        if self.cost_value == 0:
            return Decimal("0")
        return (self.unrealized_pnl / abs(self.cost_value)) * Decimal("100")

    @property
    def is_long(self) -> bool:
        """Check if this is a long position"""
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        """Check if this is a short position"""
        return self.quantity < 0

    @property
    def description(self) -> str:
        """Get a human-readable description of the position"""
        if self.instrument.type == InstrumentType.STOCK:
            return f"{self.quantity:,.0f} {self.instrument.symbol}"
        else:
            opt = self.instrument.option_details
            direction = "long" if self.is_long else "short"
            return (
                f"{abs(self.quantity):,.0f} {direction} {self.instrument.symbol} "
                f"{opt.option_type.value} {opt.strike:,.2f} {opt.expiry:%Y-%m-%d}"
            )

    def to_dict(self) -> dict:
        """Convert position to dictionary for serialization"""
        base_dict = {
            "symbol": self.instrument.symbol,
            "type": self.instrument.type.value,
            "quantity": str(self.quantity),
            "cost_basis": str(self.cost_basis),
            "market_price": str(self.market_price),
            "market_value": str(self.market_value),
            "unrealized_pnl": str(self.unrealized_pnl),
            "unrealized_pnl_percent": str(self.unrealized_pnl_percent),
        }

        if self.instrument.type == InstrumentType.OPTION:
            opt = self.instrument.option_details
            base_dict.update(
                {
                    "strike": str(opt.strike),
                    "expiry": opt.expiry.isoformat(),
                    "option_type": opt.option_type.value,
                }
            )

        return base_dict
