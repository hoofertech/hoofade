from typing import Optional
from models.trade import Trade
from models.message import Message
from models.instrument import InstrumentType, OptionType, Instrument
from datetime import timedelta


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

        # Calculate padding for alignment
        symbol_width = len(symbol_text)
        quantity_width = len(str(int(abs(trade.quantity))))
        price_width = len(f"{trade.price:.2f}")

        content = (
            f"🚨 {action:<4} "  # Fixed width for Buy/Sell
            f"{symbol_text:<{symbol_width}} "
            f"{int(abs(trade.quantity)):>{quantity_width}}"
            f"@{currency_symbol}{trade.price:>{price_width}.2f}"
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

        currency = trade.currency
        currency_symbol = (
            "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
        )
        symbol_text = self._format_instrument(trade.instrument, currency_symbol)
        pl_sign = "+" if pl_pct > 0 else ""
        action = "Buy" if trade.side == "BUY" else "Sell"

        # Calculate padding for alignment
        symbol_width = len(symbol_text)
        quantity_width = len(str(int(abs(trade.quantity))))
        price_width = len(f"{trade.price:.2f}")
        pl_width = len(f"{abs(pl_pct):.2f}")

        content = (
            f"📊 {action:<4} "  # Fixed width for Buy/Sell
            f"(closed: {hold_time_str}) "
            f"{symbol_text:<{symbol_width}} "
            f"{int(abs(trade.quantity)):>{quantity_width}} "
            f"@ {currency_symbol}{trade.price:>{price_width}.2f} "
            f"-> {pl_sign}{pl_pct:>{pl_width}.2f}%"
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

    def _format_instrument(self, instrument: Instrument, currency_symbol: str) -> str:
        max_symbol = 4
        if instrument.type == InstrumentType.STOCK:
            return f"${instrument.symbol:<{max_symbol}}"
        elif instrument.type == InstrumentType.OPTION and instrument.option_details:
            expiry = instrument.option_details.expiry.strftime("%d%b%y").upper()
            strike = instrument.option_details.strike
            option_type = (
                "C" if instrument.option_details.option_type == OptionType.CALL else "P"
            )
            return f"${instrument.symbol:<{max_symbol}} {expiry} {currency_symbol}{strike}{option_type}"
        return f"${instrument.symbol:<{max_symbol}}"

    def _format_hold_time(self, hold_time: timedelta) -> str:
        days = hold_time.days
        hours = hold_time.seconds // 3600
        minutes = (hold_time.seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days} {'day' if days == 1 else 'days'}")
        if hours > 0:
            parts.append(f"{hours} {'hour' if hours == 1 else 'hours'}")
        if minutes > 0:
            parts.append(f"{minutes} {'minute' if minutes == 1 else 'minutes'}")

        return " ".join(parts) if parts else "0 minutes"
