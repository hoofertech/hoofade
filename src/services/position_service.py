from typing import Dict
from datetime import datetime, timezone
from sources.base import TradeSource
from formatters.portfolio import PortfolioFormatter
from sinks.base import MessageSink


class PositionService:
    def __init__(self, sources: Dict[str, TradeSource], sinks: Dict[str, MessageSink]):
        self.sources = sources
        self.sinks = sinks
        self.portfolio_formatter = PortfolioFormatter()
        self.last_portfolio_post = None

    async def publish_portfolio(self, source: TradeSource):
        positions = source.get_positions()
        timestamp = datetime.now(timezone.utc)
        message = self.portfolio_formatter.format_portfolio(positions, timestamp)

        for sink in self.sinks.values():
            if sink.can_publish():
                await sink.publish(message)

        self.last_portfolio_post = timestamp

    def should_post_portfolio(self, now: datetime) -> bool:
        if self.last_portfolio_post is None:
            return True
        last_post_day = self.last_portfolio_post.date()
        current_day = now.date()
        return current_day > last_post_day
