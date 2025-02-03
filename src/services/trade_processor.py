from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict, Union, Tuple
from datetime import datetime
from models.trade import Trade
from models.position import Position
from models.instrument import Instrument, InstrumentType
import logging

logger = logging.getLogger(__name__)


@dataclass
class CombinedTrade:
    """Represents multiple trades of the same instrument combined together"""

    instrument: Instrument
    quantity: Decimal
    weighted_price: Decimal
    trades: List[Trade]
    timestamp: datetime  # Latest timestamp from combined trades
    currency: str
    side: str  # Explicit "BUY" or "SELL"


@dataclass
class ProfitTaker:
    """Represents a pair of trades that close a position"""

    buy_trade: CombinedTrade
    sell_trade: CombinedTrade
    profit_amount: Decimal
    profit_percentage: Decimal

    @property
    def timestamp(self) -> datetime:
        """Calculate the latest timestamp between buy and sell trades."""
        return max(self.buy_trade.timestamp, self.sell_trade.timestamp)

    @property
    def instrument(self) -> Instrument:
        """Get the instrument from the buy or sell trade"""
        return self.buy_trade.instrument

    @property
    def currency(self) -> str:
        """Get the currency from the buy or sell trade"""
        return self.buy_trade.currency


ProcessingResult = Union[Trade, CombinedTrade, ProfitTaker]


class TradeProcessor:
    def __init__(self, portfolio: List[Position]):
        """Initialize with current portfolio positions"""
        self.portfolio = {
            self._get_instrument_key_from_instrument(position.instrument): position
            for position in portfolio
        }

    def process_trades(
        self, trades: List[Trade]
    ) -> Tuple[List[ProcessingResult], List[ProfitTaker]]:
        if not trades:
            return [], []

        results: list[ProcessingResult] = []

        # 1. Sort and group trades by complete instrument details
        grouped_trades = self._group_trades(trades)

        # 2. Combine same-direction trades
        combined_trades = self._combine_trades(grouped_trades)

        # 3. Generate profit takers and get remaining trades
        profit_takers, remaining_after_profit = self._generate_profit_takers(
            combined_trades
        )
        results.extend(profit_takers)

        # 4. Match remaining trades with portfolio positions
        portfolio_matches, remaining_after_portfolio = self._match_with_portfolio(
            remaining_after_profit
        )
        results.extend(portfolio_matches)

        # Sort results by symbol and then timestamp
        results.sort(key=lambda x: (x.instrument.symbol, x.timestamp))

        # Sort remaining_after_portfolio by symbol and then timestamp
        remaining_sorted = sorted(
            [(symbol, trades) for symbol, trades in remaining_after_portfolio.items()],
            key=lambda x: x[0],
            reverse=True,
        )

        # 5. Add any remaining unmatched trades
        for _, rem_trade in remaining_sorted:
            results = rem_trade + results

        return results, portfolio_matches

    def _get_instrument_key_from_instrument(self, instrument: Instrument) -> str:
        """Generate a unique key for an instrument including all its details"""
        if instrument.type == InstrumentType.STOCK:
            return f"stock_{instrument.symbol}"
        elif instrument.type == InstrumentType.OPTION and instrument.option_details:
            details = instrument.option_details
            return (
                f"option_{instrument.symbol}_"
                f"{details.strike}_{details.expiry}_{details.option_type}"
            )
        return f"other_{instrument.symbol}"

    def _get_instrument_key(self, trade: Union[Trade, Position]) -> str:
        """Generate a unique key for an instrument including all its details"""
        return self._get_instrument_key_from_instrument(trade.instrument)

    def _group_trades(self, trades: List[Trade]) -> Dict[str, List[Trade]]:
        """Group trades by complete instrument details"""
        grouped = {}
        for trade in sorted(
            trades, key=lambda t: (t.instrument.symbol, -t.timestamp.timestamp())
        ):
            key = self._get_instrument_key(trade)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(trade)
        return grouped

    def _combine_trades(
        self, grouped_trades: Dict[str, List[Trade]]
    ) -> Dict[str, List[CombinedTrade]]:
        """Combine trades of the same direction for each instrument"""
        combined = {}
        for instrument_key, trades in grouped_trades.items():
            buys = []
            sells = []
            for trade in trades:
                if trade.side == "BUY":
                    buys.append(trade)
                else:
                    sells.append(trade)

            combined[instrument_key] = []
            if buys:
                combined[instrument_key].append(
                    self._combine_same_direction_trades(buys, "BUY")
                )
            if sells:
                combined[instrument_key].append(
                    self._combine_same_direction_trades(sells, "SELL")
                )

        return combined

    def _combine_same_direction_trades(
        self, trades: List[Trade], side: str
    ) -> CombinedTrade:
        """Combine multiple trades into a single trade with weighted average price"""
        total_quantity = Decimal(
            sum(abs(t.quantity) for t in trades)
        )  # Convert to Decimal
        weighted_sum = Decimal(
            sum(abs(t.quantity) * t.price for t in trades)
        )  # Convert to Decimal
        weighted_price = (
            weighted_sum / total_quantity if total_quantity > 0 else Decimal("0")
        )
        latest_timestamp = max(t.timestamp for t in trades)

        return CombinedTrade(
            instrument=trades[0].instrument,
            quantity=total_quantity,
            weighted_price=weighted_price,
            trades=trades,
            timestamp=latest_timestamp,
            currency=trades[0].currency,
            side=side,
        )

    def _calculate_profit_taker(
        self,
        buy_trade: CombinedTrade,
        sell_trade: CombinedTrade,
        matched_quantity: Decimal,
    ) -> ProfitTaker:
        """Calculate profit/loss for a pair of matched trades"""
        # Determine which trade came first chronologically
        first_trade = (
            buy_trade if buy_trade.timestamp < sell_trade.timestamp else sell_trade
        )
        second_trade = (
            sell_trade if buy_trade.timestamp < sell_trade.timestamp else buy_trade
        )

        # Calculate profit based on chronological order
        price_diff = second_trade.weighted_price - first_trade.weighted_price

        # Multiply by 100 for options
        contract_multiplier = (
            Decimal("100")
            if first_trade.instrument.type == InstrumentType.OPTION
            else Decimal("1")
        )
        profit_amount = price_diff * matched_quantity * contract_multiplier

        profit_percentage = (
            (second_trade.weighted_price - first_trade.weighted_price)
            / first_trade.weighted_price
            * Decimal("100")
        )

        # If the sell came first, it's a short trade, so invert the profit
        if first_trade.side == "SELL":
            profit_amount = -profit_amount
            profit_percentage = -profit_percentage

        return ProfitTaker(
            buy_trade=buy_trade,
            sell_trade=sell_trade,
            profit_amount=profit_amount,
            profit_percentage=profit_percentage,
        )

    def _generate_profit_takers(
        self, combined_trades: Dict[str, List[CombinedTrade]]
    ) -> Tuple[List[ProfitTaker], Dict[str, List[CombinedTrade]]]:
        """Generate profit takers for opposing trades and handle remaining quantities"""
        profit_takers = []
        updated_trades = {}

        for instrument_key, trades in combined_trades.items():
            if len(trades) != 2:  # If not a pair, keep as is
                updated_trades[instrument_key] = trades
                continue

            buy = next(t for t in trades if t.side == "BUY")
            sell = next(t for t in trades if t.side == "SELL")

            # Calculate matched quantity
            matched_quantity = min(buy.quantity, sell.quantity)

            # Create matched portions
            matched_buy = self._create_partial_combined_trade(buy, matched_quantity)
            matched_sell = self._create_partial_combined_trade(sell, matched_quantity)

            profit_takers.append(
                self._calculate_profit_taker(
                    matched_buy, matched_sell, matched_quantity
                )
            )

            # Handle remaining quantities
            remaining_trades = []
            if buy.quantity > matched_quantity:
                remaining_trades.append(
                    self._create_partial_combined_trade(
                        buy, buy.quantity - matched_quantity
                    )
                )
            if sell.quantity > matched_quantity:
                remaining_trades.append(
                    self._create_partial_combined_trade(
                        sell, sell.quantity - matched_quantity
                    )
                )

            if remaining_trades:
                updated_trades[instrument_key] = remaining_trades

        return profit_takers, updated_trades

    def _create_partial_combined_trade(
        self, trade: CombinedTrade, target_quantity: Decimal
    ) -> CombinedTrade:
        """Create a new CombinedTrade with only the trades needed for target quantity"""
        remaining_quantity = target_quantity
        matched_trades = []

        for t in trade.trades:
            if remaining_quantity <= 0:
                break

            trade_quantity = min(abs(t.quantity), remaining_quantity)
            remaining_quantity -= trade_quantity

            if trade_quantity == abs(t.quantity):
                matched_trades.append(t)
            else:
                # Create partial trade
                partial_trade = Trade(
                    instrument=t.instrument,
                    quantity=trade_quantity if t.quantity > 0 else -trade_quantity,
                    price=t.price,
                    timestamp=t.timestamp,
                    source_id=t.source_id,
                    trade_id=t.trade_id,
                    currency=t.currency,
                    side=trade.side,
                )
                matched_trades.append(partial_trade)

        return CombinedTrade(
            instrument=trade.instrument,
            quantity=target_quantity,
            weighted_price=trade.weighted_price,
            trades=matched_trades,
            timestamp=trade.timestamp,
            currency=trade.currency,
            side=trade.side,
        )

    def _match_with_portfolio(
        self, combined_trades: Dict[str, List[CombinedTrade]]
    ) -> Tuple[List[ProfitTaker], Dict[str, List[CombinedTrade]]]:
        """Match trades with existing portfolio positions"""
        portfolio_matches = []
        remaining_trades = {}

        for instrument_key, trades in combined_trades.items():
            position = self.portfolio.get(instrument_key)
            if not position:
                remaining_trades[instrument_key] = trades
                continue

            unmatched_trades = []
            for trade in trades:
                # Only match trades that close positions (opposite sides)
                if ((not position.is_short) and trade.side == "SELL") or (
                    position.is_short and trade.side == "BUY"
                ):
                    # Calculate matched quantity
                    matched_quantity = min(abs(position.quantity), trade.quantity)

                    # Create synthetic trade from position for matched portion
                    position_trade = CombinedTrade(
                        instrument=position.instrument,
                        quantity=matched_quantity,
                        weighted_price=position.cost_basis,
                        trades=[],  # No actual trades since this is from position
                        timestamp=trade.timestamp,
                        side="SELL" if position.is_short else "BUY",
                        currency=trade.currency,
                    )

                    # Create partial trade for matched portion
                    matched_trade = self._create_partial_combined_trade(
                        trade, matched_quantity
                    )

                    portfolio_matches.append(
                        self._calculate_profit_taker(
                            buy_trade=position_trade
                            if not position.is_short
                            else matched_trade,
                            sell_trade=matched_trade
                            if trade.side == "SELL"
                            else position_trade,
                            matched_quantity=matched_quantity,
                        )
                    )

                    # Handle remaining quantity from trade
                    if trade.quantity > matched_quantity:
                        remaining_trade = self._create_partial_combined_trade(
                            trade, trade.quantity - matched_quantity
                        )
                        unmatched_trades.append(remaining_trade)
                else:
                    # Trade doesn't close position, keep it as is
                    unmatched_trades.append(trade)

            if unmatched_trades:
                remaining_trades[instrument_key] = unmatched_trades

        return portfolio_matches, remaining_trades

    def _is_trade_in_profit_takers(
        self, trade: CombinedTrade, profit_takers: List[ProfitTaker]
    ) -> bool:
        """Check if a trade is already part of a profit taker"""
        for pt in profit_takers:
            if (trade is pt.buy_trade) or (trade is pt.sell_trade):
                return True
        return False

    def _get_symbol_from_key(self, instrument_key: str) -> str:
        """Extract symbol from instrument key"""
        parts = instrument_key.split("_")
        return parts[1] if len(parts) > 1 else parts[0]
