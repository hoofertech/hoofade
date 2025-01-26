from typing import Dict, Any
from src.sources.base import TradeSource
from src.sources.ibkr import IBKRSource


class SourceFactory:
    @staticmethod
    def create_source(source_type: str, config: Dict[str, Any]) -> TradeSource:
        if source_type == "ibkr":
            return IBKRSource(
                source_id=config["source_id"],
                host=config["host"],
                port=config["port"],
                client_id=config["client_id"],
            )
        raise ValueError(f"Unknown source type: {source_type}")
