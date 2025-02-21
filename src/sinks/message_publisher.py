import copy
import logging
from datetime import datetime, timedelta
from typing import List

from formatters.message_splitter import MessageSplitter
from formatters.portfolio import PortfolioFormatter
from formatters.trade import TradeFormatter
from models.message import Message
from models.position import Position
from models.trade import Trade
from services.position_service import PositionService
from services.trade_processor import TradeProcessor
from sinks.base import MessageSink

logger = logging.getLogger(__name__)


class MessagePublisher(MessageSink):
    def __init__(self, sink_id: str):
        MessageSink.__init__(self, sink_id)
        self.trade_formatter = TradeFormatter()
        self.portfolio_formatter = PortfolioFormatter()
        self.positions = []

    async def create_trade_message(self, trades: List[Trade], now: datetime) -> Message | None:
        if not trades:
            return None

        # Process trades to get combined trades and profit takers
        processor = TradeProcessor(self.positions)
        processed_results, _ = processor.process_trades(trades)

        # Get timestamp of most recent trade
        last_trade_timestamp = max(trade.timestamp for trade in processed_results)
        date_str = last_trade_timestamp.strftime("%d %b %Y %H:%M").upper()
        # Format processed_results into messages
        content = [
            f"Trades on {date_str}",
            "",  # Empty line after header
        ]

        for msg in self.trade_formatter.format_trades(processed_results):
            content.append(msg.content)

        # Create combined message
        combined_message = Message(
            content="\n".join(content),
            timestamp=last_trade_timestamp,
            metadata={"type": "trd"},
        )

        new_trades = trades
        if new_trades:
            logger.info(f"Applying {len(new_trades)} trades to portfolio")
            for new_trade in new_trades:
                await PositionService.apply_new_trade(new_trade, self.positions)

            logger.info(f"Published {len(new_trades)} trades.")
            if self.PUBLISH_PORTFOLIO_AFTER_EACH_TRADE:
                last_trade_timestamp = max(trade.timestamp for trade in new_trades)
                await self.publish_portfolio(
                    self.positions, last_trade_timestamp + timedelta(seconds=1)
                )

        return combined_message

    def create_portfolio_message(self, positions: List[Position], now: datetime) -> Message:
        return self.portfolio_formatter.format_portfolio(positions, now)

    def split_message(self, message: Message) -> List[Message]:
        return MessageSplitter.split_to_tweets(message)

    def update_portfolio(self, positions: List[Position]) -> bool:
        self.positions = copy.deepcopy(positions)
        return True
