from datetime import datetime
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
        self.positions: List[Position] = []
        self.last_day_trades: List[Trade] = []

    async def load_positions(self) -> bool:
        try:
            # Test connection by fetching positions
            self.positions = [pos async for pos in self.flex_client.get_positions()]
            logger.info(f"Connected to IBKR Flex: {len(self.positions)} positions")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IBKR Flex: {str(e)}")
            return False

    def get_positions(self) -> List[Position]:
        return self.positions

    async def load_last_day_trades(self) -> bool:
        try:
            executions = [
                exec async for exec in self.flex_client.get_trades(self.source_id)
            ]
            if not executions:
                return True
            since = self.get_min_datetime_for_last_day(executions)
            self.last_day_trades = [
                exec for exec in executions if exec.timestamp >= since
            ]
            return True
        except Exception as e:
            logger.error(f"Error fetching trades: {str(e)}")
            return False

    def get_last_day_trades(self) -> List[Trade]:
        return self.last_day_trades

    @staticmethod
    def get_min_datetime_for_last_day(executions: List[Trade]) -> datetime:
        last_day_in_data = max(exec.timestamp for exec in executions)
        return last_day_in_data.replace(hour=0, minute=0, second=0, microsecond=0)

    def is_done(self) -> bool:
        return False

    def get_sleep_time(self) -> int:
        return 900
