from datetime import datetime
from typing import AsyncIterator
import logging
from models.trade import Trade
from sources.base import TradeSource
import asyncio
from typing import List
from models.position import Position

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
        self.positions = []

    async def load_positions(self) -> bool:
        try:
            # Test connection by fetching positions
            self.positions = [pos async for pos in self.flex_client.get_positions()]
            logger.info(f"Connected to IBKR Flex: {len(self.positions)} positions")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR Flex: {str(e)}")
            return False

    async def get_positions(self) -> AsyncIterator[Position]:
        try:
            for pos in self.positions:
                yield pos
        except Exception as e:
            logger.error(f"Failed to fetch positions: {str(e)}")
            return

    async def get_last_day_trades(self) -> AsyncIterator[Trade]:
        try:
            executions = [
                exec async for exec in self.flex_client.get_trades(self.source_id)
            ]
            if not executions:
                return
            since = self.get_min_datetime_for_last_day(executions)
            for exec in executions:
                if exec.timestamp >= since:
                    yield exec
        except Exception as e:
            logger.error(f"Error fetching trades: {str(e)}")

    @staticmethod
    def get_min_datetime_for_last_day(executions: List[Trade]) -> datetime:
        last_day_in_data = max(exec.timestamp for exec in executions)
        return last_day_in_data.replace(hour=0, minute=0, second=0, microsecond=0)

    def is_done(self) -> bool:
        return False
