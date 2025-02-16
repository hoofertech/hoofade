import logging
from datetime import datetime
from typing import Any, Tuple, override

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

    @override
    async def load_latest_positions_data(self) -> Tuple[dict[str, Any] | None, datetime | None]:
        return await self.flex_client.download_positions()

    @override
    async def load_latest_trades_data(self) -> Tuple[list[dict[str, Any]] | None, datetime | None]:
        data, when_generated = await self.flex_client.download_trades()
        if not data:
            return None, when_generated
        return data.get("TradeConfirm", []), when_generated

    @override
    def is_done(self) -> bool:
        return False

    @override
    def get_sleep_time(self) -> int:
        return 900
