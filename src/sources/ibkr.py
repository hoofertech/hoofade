from datetime import datetime
from typing import Iterator
import logging
from ib_insync import IB
from src.models.trade import Trade
from src.sources.base import TradeSource
from decimal import Decimal

logger = logging.getLogger(__name__)


class IBKRSource(TradeSource):
    def __init__(self, source_id: str, host: str, port: int, client_id: int):
        super().__init__(source_id)
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()

    def connect(self) -> bool:
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {str(e)}")
            return False

    def get_recent_trades(self, since: datetime) -> Iterator[Trade]:
        trades = self.ib.trades()
        for trade in trades:
            if trade.time >= since:
                yield Trade(
                    symbol=trade.contract.symbol,
                    quantity=Decimal(str(trade.execution.shares)),
                    price=Decimal(str(trade.execution.price)),
                    side="BUY" if trade.execution.side == "BOT" else "SELL",
                    timestamp=trade.time,
                    source_id=self.source_id,
                    trade_id=str(trade.execution.execId),
                )

    def disconnect(self) -> None:
        self.ib.disconnect()
