from typing import Dict, Any
from src.sinks.base import MessageSink
from src.sinks.twitter import TwitterSink


class SinkFactory:
    @staticmethod
    def create_sink(sink_type: str, config: Dict[str, Any]) -> MessageSink:
        if sink_type == "twitter":
            return TwitterSink(
                sink_id=config["sink_id"],
                credentials={
                    "bearer_token": config["bearer_token"],
                    "consumer_key": config["api_key"],
                    "consumer_secret": config["api_secret"],
                    "access_token": config["access_token"],
                    "access_token_secret": config["access_token_secret"],
                },
            )
        raise ValueError(f"Unknown sink type: {sink_type}")
