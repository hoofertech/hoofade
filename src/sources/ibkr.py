from datetime import datetime
from typing import AsyncIterator
import logging
from models.trade import Trade
from sources.base import TradeSource
import asyncio

# Set up event loop
try:
    _event_loop = asyncio.get_running_loop()
except RuntimeError:
    _event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_event_loop)

from .flex_client import FlexClient, FlexQueryConfig, FlexClientConfig

logger = logging.getLogger(__name__)


class IBKRSource(TradeSource):
    def __init__(
        self,
        source_id: str,
        portfolio_token: str,
        portfolio_query_id: str,
        trades_token: str,
        trades_query_id: str,
        save_dir: str | None = None,
    ):
        super().__init__(source_id)
        self.flex_client = FlexClient(
            FlexClientConfig(
                portfolio=FlexQueryConfig(
                    token=portfolio_token, query_id=portfolio_query_id
                ),
                trades=FlexQueryConfig(token=trades_token, query_id=trades_query_id),
                save_dir=save_dir,
            )
        )

    async def connect(self) -> bool:
        try:
            # Test connection by fetching positions
            await self.flex_client.get_positions()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR Flex: {str(e)}")
            return False

    async def get_recent_trades(self, since: datetime) -> AsyncIterator[Trade]:
        try:
            executions = await self.flex_client.get_executions()
            for exec in executions:
                if exec.timestamp >= since:
                    yield Trade(
                        instrument=exec.instrument,
                        quantity=exec.quantity,
                        price=exec.price,
                        currency=exec.currency,
                        side=exec.side,
                        timestamp=exec.timestamp,
                        source_id=self.source_id,
                        trade_id=exec.exec_id,
                    )
        except Exception as e:
            logger.error(f"Error fetching trades: {str(e)}")

    async def disconnect(self) -> None:
        # No cleanup needed for Flex API
        pass
