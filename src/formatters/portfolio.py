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

        # Format stock positions
        if stock_positions:
            stock_content = "📈 Stock Positions:\n"
            for pos in stock_positions:
                currency = pos.instrument.currency
                currency_symbol = (
                    "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
                )
                stock_content += f"${pos.instrument.symbol}: {int(abs(pos.quantity))}@{currency_symbol}{pos.market_price}\n"

            messages.append(
                Message(
                    content=stock_content.strip(),
                    timestamp=timestamp,
                    metadata={"type": "stock_portfolio"},
                )
            )

        # Format option positions
        if option_positions:
            option_content = "🎯 Option Positions:\n"
            current_expiry = None

            for pos in option_positions:
                if not pos.instrument.option_details:
                    continue

                expiry = pos.instrument.option_details.expiry
                if current_expiry != expiry:
                    if current_expiry is not None:
                        option_content += "\n"
                    current_expiry = expiry
                    option_content += f"{expiry.strftime('%d%b%Y').upper()}:\n"

                currency = pos.instrument.currency
                currency_symbol = (
                    "$" if currency == "USD" else "€" if currency == "EUR" else "¥"
                )
                strike = pos.instrument.option_details.strike
                option_type = (
                    "C"
                    if pos.instrument.option_details.option_type == OptionType.CALL
                    else "P"
                )

                option_content += (
                    f"${pos.instrument.symbol} {currency_symbol}{strike}{option_type}: "
                    f"{int(abs(pos.quantity))}@{currency_symbol}{pos.market_price}\n"
                )

            messages.append(
                Message(
                    content=option_content.strip(),
                    timestamp=timestamp,
                    metadata={"type": "option_portfolio"},
                )
            )

        return messages
