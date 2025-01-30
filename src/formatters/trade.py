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
        currency = trade.currency
        currency_symbol = (
            "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
        )
        symbol_text = self._format_instrument(trade.instrument, currency_symbol)
        action = "Buy" if trade.side == "BUY" else "Sell"

        content = (
            f"🚨 {action} {symbol_text}: {int(abs(trade.quantity))}@${trade.price}"
        )
        content = f"🚨 {action} {symbol_text}: {int(abs(trade.quantity))}@{currency_symbol}{trade.price}"

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

        currency = trade.currency
        currency_symbol = (
            "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
        )
        symbol_text = self._format_instrument(trade.instrument, currency_symbol)
        pl_sign = "+" if pl_pct > 0 else ""

        action = "Buy" if trade.side == "BUY" else "Sell"

        content = f"📊 {action} (closed: {hold_time_str}) {symbol_text}: {int(abs(trade.quantity))}@{currency_symbol}{trade.price} -> {pl_sign}{pl_pct:.2f}%"

        return Message(
            content=content,
            timestamp=trade.timestamp,
            metadata={
                "trade_id": trade.trade_id,
                "matching_trade_id": matching_trade.trade_id,
                "pl_pct": pl_pct,
            },
        )

    def _format_instrument(self, instrument: Instrument, currency_symbol: str) -> str:
        if instrument.type == InstrumentType.STOCK:
            return f"${instrument.symbol}"

        if not instrument.option_details:
            raise ValueError("Option details missing for option instrument")

        expiry = instrument.option_details.expiry.strftime("%d-%b-%Y")
        strike = instrument.option_details.strike
        option_type = (
            "C" if instrument.option_details.option_type == OptionType.CALL else "P"
        )

        return f"${instrument.symbol}/{expiry}@{currency_symbol}{strike}{option_type}"

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
