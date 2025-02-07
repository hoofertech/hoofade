from typing import Union, List
from models.trade import Trade
from models.message import Message
from models.instrument import Instrument, InstrumentType, OptionType
from services.trade_processor import CombinedTrade, ProfitTaker
import logging
from decimal import Decimal
from datetime import datetime
from services.trade_processor import ProcessingResult


logger = logging.getLogger(__name__)


class TradeFormatter:
    def __init__(self):
        self.total_profit = Decimal("0")
        self.total_trades = 0
        self.profitable_trades = 0

    def format_trades(self, trades: List[ProcessingResult]) -> List[Message]:
        """Format a list of trades into messages"""
        # Reset totals for new batch
        self.total_profit = Decimal("0")
        self.total_trades = 0
        self.profitable_trades = 0

        messages = []
        for trade in trades:
            if isinstance(trade, ProfitTaker):
                self.total_profit += trade.profit_amount
                trade_count = int(
                    (len(trade.buy_trade.trades) if trade.buy_trade.trades else 0)
                    + (len(trade.sell_trade.trades) if trade.sell_trade.trades else 0)
                )
                self.total_trades += trade_count
                if trade.profit_percentage > 0:
                    self.profitable_trades += trade_count
            messages.append(self._format_trade(trade))

        # Add summary message if there were any profit/loss trades
        if self.total_trades > 0:
            messages.append(self._create_summary_message())

        return messages

    def _create_summary_message(self) -> Message:
        """Create a summary message for all profit/loss trades"""
        is_profit = self.total_profit > 0
        pl_emoji = "📈" if is_profit else "📉"
        pl_text = "PROFIT" if is_profit else "LOSS"

        win_rate = (
            (self.profitable_trades / self.total_trades * 100)
            if self.total_trades > 0
            else 0
        )

        content = [
            f"{pl_emoji} Total {pl_text}: ${abs(self.total_profit):.2f}",
            f"Win Rate: {win_rate:.1f}% ({self.profitable_trades}/{self.total_trades} closed trades)",
        ]

        return Message(
            content="\n".join(content),
            timestamp=datetime.now(),
            metadata={
                "type": "profit_summary",
                "total_profit": self.total_profit,
                "total_trades": self.total_trades,
                "profitable_trades": self.profitable_trades,
                "win_rate": win_rate,
            },
        )

    def _format_trade(self, trade: Union[Trade, CombinedTrade, ProfitTaker]) -> Message:
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
            content=content,
            timestamp=timestamp,
            metadata={"type": "trade", "trade_id": trade_id},
        )

    def _format_profit_taker(self, profit_taker: ProfitTaker) -> Message:
        """Format a profit taker with its component trades"""
        is_profit = profit_taker.profit_percentage > 0
        pl_sign = "+" if is_profit else ""
        pl_amount_sign = "+" if is_profit else "-"
        pl_text = "PROFIT" if is_profit else "LOSS"
        pl_emoji = "📈" if is_profit else "📉"

        currency = profit_taker.currency
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
        pl_width = len(f"{abs(profit_taker.profit_percentage):.2f}")

        # Main profit/loss line
        content = [
            f"{pl_emoji} {pl_text} {symbol_text:<{symbol_width}} "
            f"{int(quantity):>{quantity_width}} "
            f"-> {pl_sign}{profit_taker.profit_percentage:>{pl_width}.2f}% "
            f"({pl_amount_sign}{currency_symbol}{abs(profit_taker.profit_amount):.2f})",
        ]

        # Add component trades indented
        content.extend(
            self._format_component_trades(
                profit_taker.buy_trade, profit_taker.sell_trade
            )
        )

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
        self, buy_trade: CombinedTrade, sell_trade: CombinedTrade
    ) -> List[str]:
        """Format individual trades that make up buy and sell trades in chronological order"""
        lines = []
        currency = (
            buy_trade.trades[0].currency
            if buy_trade.trades
            else sell_trade.trades[0].currency
            if sell_trade.trades
            else "USD"
        )
        currency_symbol = self._get_currency_symbol(currency)

        # Combine all trades and sort by timestamp
        all_trades = []

        # Helper function to consolidate trades
        def add_trades_consolidated(
            trades: List[Trade], side: str, from_position: bool = False
        ):
            if not trades:
                return

            # Group trades by price and timestamp
            grouped = {}
            for trade in trades:
                key = (trade.price, trade.timestamp if not from_position else None)
                if key not in grouped:
                    grouped[key] = Decimal("0")
                grouped[key] += abs(trade.quantity)

            # Add consolidated trades
            for (price, timestamp), total_quantity in grouped.items():
                all_trades.append(
                    (side, timestamp, total_quantity, price, from_position)
                )

        # Handle buy position
        if not buy_trade.trades:
            all_trades.append(
                ("BUY", None, buy_trade.quantity, buy_trade.weighted_price, True)
            )
        else:
            add_trades_consolidated(buy_trade.trades, "BUY")

        # Handle sell position
        if not sell_trade.trades:
            all_trades.append(
                ("SELL", None, sell_trade.quantity, sell_trade.weighted_price, True)
            )
        else:
            add_trades_consolidated(sell_trade.trades, "SELL")

        # Sort trades - positions (from_position=True) first, then by timestamp
        all_trades.sort(key=lambda x: (not x[4], x[1] or datetime.min))

        # Format each trade
        for i, (side, timestamp, quantity, price, from_position) in enumerate(
            all_trades
        ):
            prefix = "    └─ " if i == len(all_trades) - 1 else "    ├─ "

            if from_position:
                lines.append(
                    f"{prefix}{side:<4} {int(quantity)} @ "
                    f"{currency_symbol}{price:.2f} (from position)"
                )
            else:
                lines.append(
                    f"{prefix}{side:<4} {int(quantity)} @ "
                    f"{currency_symbol}{price:.2f} "
                    f"({timestamp.strftime('%H:%M:%S')})"
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

    def _get_currency_symbol(self, currency: str) -> str:
        """Get currency symbol for given currency code"""
        return {"USD": "$", "EUR": "€", "JPY": "¥"}.get(currency, "$")
