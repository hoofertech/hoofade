from typing import Optional
from models.trade import Trade
from models.message import Message
from models.instrument import InstrumentType, OptionType, Instrument


class TradeFormatter:
    def format_trade(
        self, trade: Trade, matching_trade: Optional[Trade] = None
    ) -> Message:
        if matching_trade:
            return self._format_closed_position(trade, matching_trade)
        return self._format_new_trade(trade)

    def _format_new_trade(self, trade: Trade) -> Message:
        symbol_text = self._format_instrument(trade.instrument)
        action_emoji = "📈" if trade.side == "BUY" else "📉"

        content = (
            f"New Trade Alert 🚨\n"
            f"{symbol_text}\n"
            f"{action_emoji} {trade.side.capitalize()} "
            f"{abs(trade.quantity)} "
            f"{'contracts' if trade.instrument.type == InstrumentType.OPTION else 'shares'} "
            f"@ ${trade.price}"
        )

        return Message(
            content=content,
            timestamp=trade.timestamp,
            metadata={"trade_id": trade.trade_id},
        )

    def _format_closed_position(self, trade: Trade, matching_trade: Trade) -> Message:
        entry_price = matching_trade.price
        exit_price = trade.price
        pl_pct = ((exit_price / entry_price) - 1) * 100
        if trade.side == "SELL":
            pl_pct = -pl_pct

        hold_time = trade.timestamp - matching_trade.timestamp
        hold_time_str = self._format_hold_time(hold_time)
        symbol_text = self._format_instrument(trade.instrument)

        content = (
            f"Position Closed 📊\n"
            f"{symbol_text}\n"
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

    def _format_instrument(self, instrument: Instrument) -> str:
        if instrument.type == InstrumentType.STOCK:
            return f"${instrument.symbol}"

        if not instrument.option_details:
            raise ValueError("Option details missing for option instrument")

        expiry = instrument.option_details.expiry.strftime("%d %b %Y")
        strike = instrument.option_details.strike
        option_type = (
            "📞" if instrument.option_details.option_type == OptionType.CALL else "📝"
        )

        return f"${instrument.symbol} {expiry} ${strike} {option_type}"

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
