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


ProcessingResult = Union[Trade, CombinedTrade, ProfitTaker]


class TradeProcessor:
    def __init__(self, portfolio: List[Position]):
        logger.info(f"Full portfolio: {portfolio}")
        """Initialize with current portfolio positions"""
        self.portfolio = {
            self._get_instrument_key_from_instrument(position.instrument): position
            for position in portfolio
        }

    def process_trades(self, trades: List[Trade]) -> List[ProcessingResult]:
        if not trades:
            return []

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
        logger.info(f"Remaining after profit: {profit_takers}")

        # 4. Match remaining trades with portfolio positions
        portfolio_matches, remaining_after_portfolio = self._match_with_portfolio(
            remaining_after_profit
        )
        results.extend(portfolio_matches)

        logger.info(f"Remaining after portfolio: {portfolio_matches}")
        # 5. Add any remaining unmatched trades
        for rem_trade in remaining_after_portfolio.values():
            results.extend(rem_trade)

        return results

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

    def _generate_profit_takers(
        self, combined_trades: Dict[str, List[CombinedTrade]]
    ) -> Tuple[List[ProfitTaker], Dict[str, List[CombinedTrade]]]:
        """
        Generate profit takers for opposing trades and handle remaining quantities
        Returns both profit takers and updated combined trades dict
        """
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

            # Determine which trade came first
            first_trade = (
                matched_buy
                if matched_buy.timestamp < matched_sell.timestamp
                else matched_sell
            )
            second_trade = (
                matched_sell
                if matched_buy.timestamp < matched_sell.timestamp
                else matched_buy
            )

            # Calculate profit based on chronological order
            profit_amount = (
                second_trade.weighted_price - first_trade.weighted_price
            ) * matched_quantity
            profit_percentage = (
                (second_trade.weighted_price - first_trade.weighted_price)
                / first_trade.weighted_price
                * Decimal("100")
            )

            # If the sell came first, it's a short trade, so invert the profit
            if first_trade.side == "SELL":
                profit_amount = -profit_amount
                profit_percentage = -profit_percentage

            profit_takers.append(
                ProfitTaker(
                    buy_trade=matched_buy,
                    sell_trade=matched_sell,
                    profit_amount=profit_amount,
                    profit_percentage=profit_percentage,
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
        """
        Match trades with existing portfolio positions
        Returns both profit takers and remaining unmatched trades
        """
        portfolio_matches = []
        remaining_trades = {}

        for instrument_key, trades in combined_trades.items():
            position = self.portfolio.get(instrument_key)
            logger.info(f"Position: {position}")
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

                    # Calculate profit
                    if trade.side == "SELL":
                        profit_amount = (
                            trade.weighted_price - position.cost_basis
                        ) * matched_quantity
                        profit_percentage = (
                            (trade.weighted_price - position.cost_basis)
                            / position.cost_basis
                            * Decimal("100")
                        )
                    else:  # BUY
                        profit_amount = (
                            position.cost_basis - trade.weighted_price
                        ) * matched_quantity
                        profit_percentage = (
                            (position.cost_basis - trade.weighted_price)
                            / trade.weighted_price
                            * Decimal("100")
                        )

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
                        ProfitTaker(
                            buy_trade=position_trade
                            if not position.is_short
                            else matched_trade,
                            sell_trade=matched_trade
                            if trade.side == "SELL"
                            else position_trade,
                            profit_amount=profit_amount,
                            profit_percentage=profit_percentage,
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
