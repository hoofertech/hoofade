import logging
from datetime import datetime
from typing import List, Tuple

from models.position import Position
from models.trade import Trade
from sources.base import TradeSource

from .flex_client import FlexClient, FlexClientConfig, FlexQueryConfig

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
                portfolio=FlexQueryConfig(token=portfolio_token, query_id=portfolio_query_id),
                trades=FlexQueryConfig(token=trades_token, query_id=trades_query_id),
                save_dir=save_dir,
            )
        )
        self.positions: List[Position] = []
        self.last_day_trades: List[Trade] = []

    async def load_positions(self) -> Tuple[bool, datetime | None]:
        try:
            # Test connection by fetching positions
            self.positions, when_generated = await self.flex_client.download_positions()
            logger.info(f"Connected to IBKR Flex: {len(self.positions)} positions")
            return True, when_generated
        except Exception as e:
            logger.error(f"Failed to connect to IBKR Flex: {str(e)}")
            return False, None

    def get_positions(self) -> List[Position]:
        return self.positions

    async def load_last_day_trades(self) -> Tuple[bool, datetime | None]:
        executions, when_generated = await self.flex_client.download_trades(self.source_id)
        if not executions:
            return True, when_generated
        since = self.get_min_datetime_for_last_day(executions)
        self.last_day_trades = [exec for exec in executions if exec.timestamp >= since]
        return True, when_generated

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
