from datetime import datetime
from typing import AsyncIterator, Dict, Any, List
from pathlib import Path
import logging
from decimal import Decimal
from models.trade import Trade
from models.position import Position
from models.instrument import Instrument, OptionType
from .base import TradeSource
from .parser import FlexReportParser

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
        self.parser = FlexReportParser()

    def _parse_positions(self, positions_data: List[Dict[str, Any]]) -> List[Position]:
        """Parse positions from portfolio data"""
        positions = []
        for pos_data in positions_data:
            try:
                # Create instrument based on position data
                put_call = str(pos_data.get("putCall", ""))
                if put_call:
                    # Option position
                    instrument = Instrument.option(
                        symbol=str(pos_data.get("underlyingSymbol", "")),
                        strike=Decimal(str(pos_data.get("strike", "0"))),
                        expiry=datetime.strptime(
                            str(pos_data.get("expiry", "")), "%Y%m%d"
                        ).date(),
                        option_type=OptionType.CALL
                        if put_call == "C"
                        else OptionType.PUT,
                    )
                else:
                    # Stock position
                    instrument = Instrument.stock(
                        symbol=str(pos_data.get("symbol", ""))
                    )

                positions.append(
                    Position(
                        instrument=instrument,
                        quantity=Decimal(str(pos_data.get("position", "0"))),
                        cost_basis=Decimal(str(pos_data.get("costBasisPrice", "0"))),
                        market_price=Decimal(str(pos_data.get("markPrice", "0"))),
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
            portfolio_data = self.parser.load_latest_portfolio(self.data_dir)
            if portfolio_data is None:
                logger.warning("No portfolio data found")
                self.positions = []
            else:
                self.positions = self._parse_positions([portfolio_data])
                logger.info(
                    f"Loaded {len(self.positions)} positions from {self.data_dir}"
                )

            return True
        except Exception as e:
            logger.error(f"Failed to connect to JSON source: {e}")
            return False

    async def get_recent_trades(self, since: datetime) -> AsyncIterator[Trade]:
        """Get trades since the specified timestamp"""
        trade_data = self.parser.load_latest_trades(self.data_dir)
        if trade_data is None:
            return

        try:
            # Parse trade datetime
            trade_time_str = str(trade_data.get("dateTime", ""))
            trade_time = self.parser.parse_flex_datetime(trade_time_str)
            if not trade_time or trade_time < since:
                logger.info(f"No trades since {since}")
                return

            # Parse instrument
            put_call = str(trade_data.get("putCall", ""))
            if put_call:
                # Option trade
                instrument = Instrument.option(
                    symbol=str(trade_data.get("underlyingSymbol", "")),
                    strike=Decimal(str(trade_data.get("strike", "0"))),
                    expiry=datetime.strptime(
                        str(trade_data.get("expiry", "")), "%Y%m%d"
                    ).date(),
                    option_type=OptionType.CALL if put_call == "C" else OptionType.PUT,
                )
            else:
                # Stock trade
                instrument = Instrument.stock(symbol=str(trade_data.get("symbol", "")))

            yield Trade(
                instrument=instrument,
                quantity=Decimal(str(abs(float(str(trade_data.get("quantity", "0")))))),
                price=Decimal(str(trade_data.get("price", "0"))),
                side="BUY" if str(trade_data.get("buySell", "")) == "BUY" else "SELL",
                timestamp=trade_time,
                source_id=self.source_id,
                trade_id=str(trade_data.get("tradeID", "")),
            )
        except Exception as e:
            logger.error(f"Error parsing trade: {e} for data: {trade_data}")
            return

    async def disconnect(self) -> None:
        """Clean up any connections"""
        self.positions = []
