from typing import List
from models.position import Position
from models.instrument import InstrumentType, OptionType
from models.message import Message
from datetime import datetime


class PortfolioFormatter:
    def format_portfolio(
        self, positions: List[Position], timestamp: datetime
    ) -> Message:
        if not positions:
            return Message(
                content="No positions",
                timestamp=timestamp,
                metadata={"type": "portfolio"},
            )

        # Format the date in a readable format
        date_str = timestamp.strftime("%d %b %Y").upper()

        content = []
        # Add title
        content.append(f"Portfolio on {date_str}")
        content.append("")  # Empty line after title

        # Separate and sort positions
        stock_positions = [
            p for p in positions if p.instrument.type == InstrumentType.STOCK
        ]
        option_positions = [
            p for p in positions if p.instrument.type == InstrumentType.OPTION
        ]

        # Sort stock positions by symbol
        stock_positions.sort(key=lambda p: p.instrument.symbol)

        # Sort option positions by symbol, then expiry
        option_positions.sort(
            key=lambda p: (
                p.instrument.symbol,
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

            content.append("📊 Stocks:")
            for pos in stock_positions:
                if pos.quantity == 0:
                    continue

                currency = pos.instrument.currency
                currency_symbol = (
                    "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
                )
                sign = "-" if pos.quantity < 0 else "+"

                content.append(
                    f"${pos.instrument.symbol:<{max_symbol}} "
                    f"{sign}{int(abs(pos.quantity)):>{max_quantity}}"
                    f"@{currency_symbol}{pos.market_price:<{max_price}.2f}"
                )

        # Format option positions with alignment
        if option_positions:
            if stock_positions:  # Add blank line if we had stocks
                content.append("")

            # Calculate max widths for each column
            max_symbol = max(len(p.instrument.symbol) for p in option_positions)
            max_strike = max(
                len(f"{p.instrument.option_details.strike:.2f}")
                for p in option_positions
                if p.instrument.option_details
            )
            max_quantity = max(len(str(abs(int(p.quantity)))) for p in option_positions)
            max_price = max(len(f"{p.market_price:.2f}") for p in option_positions)

            content.append("🎯 Options:")
            for pos in option_positions:
                if not pos.instrument.option_details or pos.quantity == 0:
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

                content.append(
                    f"${pos.instrument.symbol:<{max_symbol}} "
                    f"{expiry} "
                    f"{currency_symbol}{strike:<{max_strike}}{option_type} "
                    f"{sign}{int(abs(pos.quantity)):>{max_quantity}}"
                    f"@{currency_symbol}{pos.market_price:<{max_price}.2f}"
                )

        return Message(
            content="\n".join(content),
            timestamp=timestamp,
            metadata={"type": "portfolio"},
        )
