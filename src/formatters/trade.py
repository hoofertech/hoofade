from typing import Union, List
from datetime import timedelta
from models.trade import Trade
from models.message import Message
from models.instrument import Instrument, InstrumentType, OptionType
from services.trade_processor import CombinedTrade, ProfitTaker
import logging

logger = logging.getLogger(__name__)


class TradeFormatter:
    def format_trade(self, trade: Union[Trade, CombinedTrade, ProfitTaker]) -> Message:
        """Format a trade, combined trade, or profit taker into a message"""
        if isinstance(trade, ProfitTaker):
            return self._format_profit_taker(trade)
        return self._format_new_trade(trade)

    def _format_new_trade(self, trade: Union[Trade, CombinedTrade]) -> Message:
        """Format a single trade or combined trade"""
        if isinstance(trade, CombinedTrade):
            currency = trade.currency
            timestamp = trade.timestamp
            trade_id = ",".join(t.trade_id for t in trade.trades)
            quantity = trade.quantity
            price = trade.weighted_price
            instrument = trade.instrument
            side = trade.side
        else:
            logger.info(f"Formatting new trade: {trade}")
            currency = trade.currency
            timestamp = trade.timestamp
            trade_id = trade.trade_id
            quantity = abs(trade.quantity)
            price = trade.price
            instrument = trade.instrument
            side = trade.side

        currency_symbol = self._get_currency_symbol(currency)
        symbol_text = self._format_instrument(instrument, currency_symbol)

        # Calculate padding for alignment
        symbol_width = len(symbol_text)
        quantity_width = len(str(int(quantity)))
        price_width = len(f"{price:.2f}")

        content = (
            f"🚨 {side:<4} "
            f"{symbol_text:<{symbol_width}} "
            f"{int(quantity):>{quantity_width}}"
            f"@{currency_symbol}{price:>{price_width}.2f}"
        )

        return Message(
            content=content, timestamp=timestamp, metadata={"trade_id": trade_id}
        )

    def _format_profit_taker(self, profit_taker: ProfitTaker) -> Message:
        """Format a profit taker with its component trades"""
        is_profit = profit_taker.profit_percentage > 0
        pl_sign = "+" if is_profit else ""
        pl_text = "PROFIT" if is_profit else "LOSS"
        pl_emoji = "📈" if is_profit else "📉"

        hold_time = profit_taker.sell_trade.timestamp - profit_taker.buy_trade.timestamp
        hold_time_str = self._format_hold_time(hold_time)

        # Get currency from first trade
        first_trade = profit_taker.buy_trade.trades[0]
        currency = first_trade.currency
        currency_symbol = self._get_currency_symbol(currency)

        symbol_text = self._format_instrument(
            profit_taker.buy_trade.instrument, currency_symbol
        )

        # Calculate padding for alignment
        quantity = min(
            profit_taker.buy_trade.quantity, profit_taker.sell_trade.quantity
        )
        symbol_width = len(symbol_text)
        quantity_width = len(str(int(quantity)))
        price_width = len(f"{profit_taker.sell_trade.weighted_price:.2f}")
        pl_width = len(f"{abs(profit_taker.profit_percentage):.2f}")

        # Main profit/loss line
        content = [
            f"{pl_emoji} {pl_text} {hold_time_str}",
            f"{symbol_text:<{symbol_width}} "
            f"{int(quantity):>{quantity_width}} @ "
            f"{currency_symbol}{profit_taker.sell_trade.weighted_price:>{price_width}.2f} "
            f"-> {pl_sign}{profit_taker.profit_percentage:>{pl_width}.2f}% "
            f"({currency_symbol}{abs(profit_taker.profit_amount):.2f})",
        ]

        # Add component trades indented
        content.extend(self._format_component_trades(profit_taker.buy_trade, "BUY"))
        content.extend(self._format_component_trades(profit_taker.sell_trade, "SELL"))

        return Message(
            content="\n".join(content),
            timestamp=profit_taker.sell_trade.timestamp,
            metadata={
                "type": "profit_taker",
                "profit_amount": profit_taker.profit_amount,
                "profit_percentage": profit_taker.profit_percentage,
                "is_profit": is_profit,
            },
        )

    def _format_component_trades(
        self, combined_trade: CombinedTrade, side: str
    ) -> List[str]:
        """Format individual trades that make up a combined trade"""
        lines = []
        currency = combined_trade.trades[0].currency if combined_trade.trades else "USD"
        currency_symbol = self._get_currency_symbol(currency)

        # If it's a position (no trades), format as single line
        if not combined_trade.trades:
            lines.append(
                f"    └─ {side:<4} {int(combined_trade.quantity)} @ "
                f"{currency_symbol}{combined_trade.weighted_price:.2f} (from position)"
            )
            return lines

        # Format each component trade
        for i, trade in enumerate(combined_trade.trades):
            prefix = "    └─ " if i == len(combined_trade.trades) - 1 else "    ├─ "
            lines.append(
                f"{prefix}{side:<4} {int(abs(trade.quantity))} @ "
                f"{currency_symbol}{trade.price:.2f} "
                f"({trade.timestamp.strftime('%H:%M:%S')})"
            )

        return lines

    def _format_instrument(self, instrument: Instrument, currency_symbol: str) -> str:
        """Format instrument symbol with option details if applicable"""
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
        """Format hold time duration in a human-readable format"""
        days = hold_time.days
        hours = hold_time.seconds // 3600
        minutes = (hold_time.seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")

        return " ".join(parts) if parts else "0m"

    def _get_currency_symbol(self, currency: str) -> str:
        """Get currency symbol for given currency code"""
        return {"USD": "$", "EUR": "€", "JPY": "¥"}.get(currency, "$")
