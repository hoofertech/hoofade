from datetime import datetime
from typing import Iterator
import logging
import asyncio
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
        # Set up event loop
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        from ib_insync import IB

        self.ib = IB()

    def connect(self) -> bool:
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {str(e)}")
            return False

    def get_recent_trades(self, since: datetime) -> Iterator[Trade]:
        from ib_insync import Trade as IBTrade

        trades: list[IBTrade] = self.ib.trades()
        for trade in trades:
            # Only process filled trades
            if not trade.isDone():
                continue

            filled_quantity = trade.filled()
            if filled_quantity == 0:
                continue

            # Get the last fill time
            last_fill = trade.fills[-1] if trade.fills else None
            if not last_fill or last_fill.execution.time < since:
                continue

            yield Trade(
                symbol=trade.contract.symbol,
                quantity=Decimal(str(filled_quantity)),
                price=Decimal(str(last_fill.execution.price)),
                side="BUY" if last_fill.execution.side == "BOT" else "SELL",
                timestamp=last_fill.execution.time,
                source_id=self.source_id,
                trade_id=str(last_fill.execution.execId),
            )

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()
        self._loop.close()
