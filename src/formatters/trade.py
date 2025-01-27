from typing import Optional
from src.models.trade import Trade
from src.models.message import Message
from src.formatters.base import MessageFormatter


class TradeFormatter(MessageFormatter):
    def __init__(self):
        self.emoji_map = {
            "BUY": "📈",
            "SELL": "📉",
            "PROFIT": "🎯",
            "LOSS": "📊",
            "BOT": "🤖",
        }

    def format_trade(
        self, trade: Trade, matching_trade: Optional[Trade] = None
    ) -> Message:
        if matching_trade:
            return self._format_closed_position(trade, matching_trade)
        return self._format_new_trade(trade)

    def _format_new_trade(self, trade: Trade) -> Message:
        content = (
            f"New Trade Alert 🚨\n"
            f"${trade.symbol}\n"
            f"{self.emoji_map[trade.side]} "
            f"{'Bought' if trade.side == 'BUY' else 'Sold'} "
            f"{abs(trade.quantity)} shares @ ${float(trade.price):.2f}"
        )
        return Message(
            content=content,
            timestamp=trade.timestamp,
            metadata={"trade_id": trade.trade_id},
        )

    def _format_closed_position(self, trade: Trade, matching_trade: Trade) -> Message:
        pnl = self._calculate_pnl(trade, matching_trade)
        content = (
            f"Position Closed {self.emoji_map['PROFIT'] if pnl > 0 else self.emoji_map['LOSS']}\n"
            f"${trade.symbol}\n"
            f"P&L: {pnl:.2f}%\n"
            f"Hold time: {self._format_hold_time(trade.timestamp - matching_trade.timestamp)}"
        )
        return Message(
            content=content,
            timestamp=trade.timestamp,
            metadata={
                "trade_id": trade.trade_id,
                "matching_trade_id": matching_trade.trade_id,
            },
        )

    def _calculate_pnl(self, current_trade: Trade, matching_trade: Trade) -> float:
        if current_trade.side == "BUY":
            return float(
                (matching_trade.price - current_trade.price) / current_trade.price * 100
            )
        return float(
            (current_trade.price - matching_trade.price) / matching_trade.price * 100
        )

    def _format_hold_time(self, delta) -> str:
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0 and days == 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        return " ".join(parts)
