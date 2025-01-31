from typing import List
from models.position import Position
from models.instrument import InstrumentType, OptionType
from models.message import Message
from datetime import datetime


class PortfolioFormatter:
    def format_portfolio(
        self, positions: List[Position], timestamp: datetime
    ) -> List[Message]:
        messages = []

        # Separate positions by type
        stock_positions = [
            p for p in positions if p.instrument.type == InstrumentType.STOCK
        ]
        option_positions = [
            p for p in positions if p.instrument.type == InstrumentType.OPTION
        ]

        # Sort stock positions by absolute value (quantity * price)
        stock_positions.sort(
            key=lambda p: abs(float(p.quantity) * float(p.market_price)), reverse=True
        )

        # Sort option positions by expiry date, then strike
        option_positions.sort(
            key=lambda p: (
                p.instrument.option_details.expiry
                if p.instrument.option_details
                else datetime.max.date(),
                float(p.instrument.option_details.strike)
                if p.instrument.option_details
                else 0,
            )
        )

        # Format stock positions with alignment
        if stock_positions:
            # Calculate max widths for each column
            max_symbol = max(len(p.instrument.symbol) for p in stock_positions)
            max_quantity = max(len(str(abs(int(p.quantity)))) for p in stock_positions)
            max_price = max(len(f"{p.market_price:.2f}") for p in stock_positions)

            stock_content = "📊 Stocks:"
            for pos in stock_positions:
                currency = pos.instrument.currency
                currency_symbol = (
                    "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
                )
                sign = "-" if pos.quantity < 0 else "+"

                stock_content += (
                    f"\n${pos.instrument.symbol:<{max_symbol}} "
                    f"{sign}{int(abs(pos.quantity)):>{max_quantity}}"
                    f"@{currency_symbol}{pos.market_price:<{max_price}.2f}"
                )

            messages.append(
                Message(
                    content=stock_content,
                    timestamp=timestamp,
                    metadata={"type": "stock_portfolio"},
                )
            )

        # Format option positions with alignment
        if option_positions:
            # Calculate max widths for each column
            max_symbol = max(len(p.instrument.symbol) for p in option_positions)
            max_strike = max(
                len(f"{p.instrument.option_details.strike}")
                for p in option_positions
                if p.instrument.option_details
            )
            max_quantity = max(len(str(abs(int(p.quantity)))) for p in option_positions)
            max_price = max(len(f"{p.market_price:.2f}") for p in option_positions)

            option_content = "🎯 Options:"
            for pos in option_positions:
                if not pos.instrument.option_details:
                    continue

                currency = pos.instrument.currency
                currency_symbol = (
                    "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
                )
                strike = pos.instrument.option_details.strike
                expiry = pos.instrument.option_details.expiry.strftime("%d%b%y").upper()
                option_type = (
                    "C"
                    if pos.instrument.option_details.option_type == OptionType.CALL
                    else "P"
                )
                sign = "-" if pos.quantity < 0 else "+"

                option_content += (
                    f"\n${pos.instrument.symbol:<{max_symbol}} "
                    f"{expiry} "
                    f"{currency_symbol}{strike:<{max_strike}}{option_type} "
                    f"{sign}{int(abs(pos.quantity)):>{max_quantity}}"
                    f"@{currency_symbol}{pos.market_price:<{max_price}.2f}"
                )

            messages.append(
                Message(
                    content=option_content,
                    timestamp=timestamp,
                    metadata={"type": "option_portfolio"},
                )
            )

        return messages
