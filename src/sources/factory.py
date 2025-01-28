from typing import Dict, Any
from sources.base import TradeSource
from sources.ibkr import IBKRSource


class SourceFactory:
    @staticmethod
    def create_source(source_type: str, config: Dict[str, Any]) -> TradeSource:
        if source_type == "ibkr":
            return IBKRSource(
                source_id=config["source_id"],
                portfolio_token=config["portfolio"]["token"],
                portfolio_query_id=config["portfolio"]["query_id"],
                trades_token=config["trades"]["token"],
                trades_query_id=config["trades"]["query_id"],
            )
        raise ValueError(f"Unknown source type: {source_type}")
