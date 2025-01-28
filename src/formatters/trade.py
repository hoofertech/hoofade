from typing import Optional
from models.trade import Trade
from models.message import Message


class TradeFormatter:
    def format_trade(
        self, trade: Trade, matching_trade: Optional[Trade] = None
    ) -> Message:
        if matching_trade:
            return self._format_closed_position(trade, matching_trade)
        return self._format_new_trade(trade)

    def _format_new_trade(self, trade: Trade) -> Message:
        content = (
            "New Trade Alert 🚨\n"
            f"${trade.symbol}\n"
            f"{'📈' if trade.side == 'BUY' else '📉'} "
            f"{trade.side.capitalize()} {abs(trade.quantity)} shares @ ${trade.price}"
        )

        return Message(
            content=content,
            timestamp=trade.timestamp,
            metadata={"trade_id": trade.trade_id},
        )

    def _format_closed_position(self, trade: Trade, matching_trade: Trade) -> Message:
        # Calculate P&L
        entry_price = matching_trade.price
        exit_price = trade.price
        pl_pct = ((exit_price / entry_price) - 1) * 100
        if trade.side == "SELL":
            pl_pct = -pl_pct

        # Calculate hold time
        hold_time = trade.timestamp - matching_trade.timestamp
        hold_time_str = self._format_hold_time(hold_time)

        content = (
            "Position Closed 📊\n"
            f"${trade.symbol}\n"
            f"P&L: {pl_pct:.2f}%\n"
            f"Hold time: {hold_time_str}"
        )

        return Message(
            content=content,
            timestamp=trade.timestamp,
            metadata={
                "trade_id": trade.trade_id,
                "matching_trade_id": matching_trade.trade_id,
                "pl_pct": pl_pct,
            },
        )

    def _format_hold_time(self, delta) -> str:
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60

        if days > 0:
            return f"{days} {'day' if days == 1 else 'days'}"
        elif hours > 0:
            if minutes > 0:
                return f"{hours} {'hour' if hours == 1 else 'hours'} {minutes} {'minute' if minutes == 1 else 'minutes'}"
            return f"{hours} {'hour' if hours == 1 else 'hours'}"
        else:
            return f"{minutes} {'minute' if minutes == 1 else 'minutes'}"
