from datetime import datetime
from typing import AsyncIterator, Dict, Any, List
import json
from pathlib import Path
import logging
from models.trade import Trade
from models.position import Position
from models.instrument import Instrument, InstrumentType, OptionDetails, OptionType
from decimal import Decimal
from .base import TradeSource
from datetime import timezone

logger = logging.getLogger(__name__)


class JsonSource(TradeSource):
    def __init__(
        self,
        source_id: str,
        data_dir: str = "data/flex_reports",
    ):
        super().__init__(source_id)
        self.data_dir = Path(data_dir)
        self.positions: List[Position] = []

    def _load_latest_portfolio(self) -> Dict[str, Any]:
        """Load the most recent portfolio file"""
        portfolio_files = sorted(self.data_dir.glob("portfolio_*.json"))
        if not portfolio_files:
            return {}

        latest_file = portfolio_files[-1]
        with open(latest_file) as f:
            return json.load(f)

    def _parse_positions(self, data: Dict[str, Any]) -> List[Position]:
        """Parse positions from portfolio data"""
        positions = []
        for pos_data in data.get(
            "OpenPosition", []
        ):  # Changed from "Position" to "OpenPosition"
            try:
                # Create instrument based on position data
                if pos_data.get("putCall"):
                    # Option position
                    option_details = OptionDetails(
                        strike=Decimal(str(pos_data["strike"])),
                        expiry=datetime.strptime(
                            pos_data["expiry"], "%Y%m%d"
                        ).date(),  # Changed date format
                        option_type=OptionType.CALL
                        if pos_data["putCall"] == "C"
                        else OptionType.PUT,
                    )
                    instrument = Instrument(
                        symbol=pos_data[
                            "underlyingSymbol"
                        ],  # Use underlyingSymbol for options
                        type=InstrumentType.OPTION,
                        option_details=option_details,
                    )
                else:
                    # Stock position
                    instrument = Instrument(
                        symbol=pos_data["symbol"], type=InstrumentType.STOCK
                    )

                positions.append(
                    Position(
                        instrument=instrument,
                        quantity=Decimal(str(pos_data["position"])),
                        cost_basis=Decimal(str(pos_data["costBasisPrice"])),
                        market_price=Decimal(str(pos_data["markPrice"])),
                    )
                )
            except Exception as e:
                logger.error(f"Error parsing position: {e}")
                continue

        return positions

    async def connect(self) -> bool:
        """Connect to the data source and load initial positions"""
        try:
            if not self.data_dir.exists():
                logger.error(f"Data directory does not exist: {self.data_dir}")
                return False

            # Load and parse positions
            portfolio_data = self._load_latest_portfolio()
            if not portfolio_data:
                logger.warning("No portfolio data found")
                self.positions = []
            else:
                self.positions = self._parse_positions(portfolio_data)
                logger.info(
                    f"Loaded {len(self.positions)} positions from {self.data_dir}"
                )

            return True
        except Exception as e:
            logger.error(f"Failed to connect to JSON source: {e}")
            return False

    def _load_latest_trades(self) -> Dict[str, Any]:
        """Load the most recent trades file"""
        trade_files = sorted(self.data_dir.glob("trades_*.json"))
        if not trade_files:
            return {}

        latest_file = trade_files[-1]
        with open(latest_file) as f:
            return json.load(f)

    async def get_recent_trades(self, since: datetime) -> AsyncIterator[Trade]:
        data = self._load_latest_trades()

        for trade_data in data.get("TradeConfirm", []):
            try:
                # Parse datetime with correct format
                trade_time = datetime.strptime(
                    trade_data["dateTime"].replace(";", " "), "%Y%m%d %H%M%S"
                ).replace(tzinfo=timezone.utc)

                if trade_time >= since:
                    if trade_data.get("putCall"):
                        # Option trade
                        option_details = OptionDetails(
                            strike=Decimal(str(trade_data["strike"])),
                            expiry=datetime.strptime(
                                str(trade_data["expiry"]), "%Y%m%d"
                            ).date(),
                            option_type=OptionType.CALL
                            if trade_data["putCall"] == "C"
                            else OptionType.PUT,
                        )
                        instrument = Instrument(
                            symbol=trade_data["underlyingSymbol"],
                            type=InstrumentType.OPTION,
                            option_details=option_details,
                        )
                    else:
                        # Stock trade
                        instrument = Instrument(
                            symbol=trade_data["symbol"], type=InstrumentType.STOCK
                        )

                    yield Trade(
                        instrument=instrument,
                        quantity=Decimal(str(abs(float(trade_data["quantity"])))),
                        price=Decimal(str(trade_data["price"])),
                        side="BUY" if trade_data["buySell"] == "BUY" else "SELL",
                        timestamp=trade_time,
                        source_id=self.source_id,
                        trade_id=str(trade_data["tradeID"]),
                    )
            except Exception as e:
                logger.error(f"Error parsing trade: {e}")
                continue

    async def disconnect(self) -> None:
        self.positions = []
