import logging
from datetime import datetime
from typing import Dict, List

from database import Database
from formatters.trade import TradeFormatter
from models.db_trade import DBTrade
from models.position import Position
from models.trade import Trade
from services.position_service import PositionService
from services.trade_processor import (
    ProfitTaker,
)
from sinks.base import MessageSink
from sources.base import TradeSource

logger = logging.getLogger(__name__)


class TradeService:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: Database,
        formatter: TradeFormatter,
        position_service: PositionService,
    ):
        self.sources = sources
        self.sinks = sinks
        self.db = db
        self.formatter = formatter
        self.position_service = position_service

    async def get_new_trades(self) -> List[Trade]:
        """Get new trades from all sources."""
        all_trades = []

        # Get and merge trades from all sources
        for source_id, source in self.sources.items():
            # Get trades
            for trade in source.get_last_day_trades():
                if not await self._is_trade_published(trade):
                    await self._save_trade(trade)
                    all_trades.append(trade)

        return all_trades

    def _apply_portfolio_match(self, match: ProfitTaker, positions: List[Position]) -> bool:
        """
        Apply a portfolio match to a list of positions.
        Returns True if match was successfully applied.

        Args:
            match: ProfitTaker containing the buy and sell trades
            positions: List of positions to search for matches

        Returns:
            bool: True if match was successfully applied to a position
        """
        # Determine which side is from position (has no trades)
        if not match.buy_trade.trades:
            position_trade = match.buy_trade
            logger.debug(f"Buy side is from position: {position_trade.instrument.symbol}")
        elif not match.sell_trade.trades:
            position_trade = match.sell_trade
            logger.debug(f"Sell side is from position: {position_trade.instrument.symbol}")
        else:
            logger.debug("No position trades found in profit taker")
            return False

        # Find matching position
        for position in positions:
            if position.instrument == position_trade.instrument:
                # Update position quantity
                position.quantity += position_trade.quantity
                logger.debug(
                    f"Applied position match for {position.instrument.symbol}: "
                    f"updated quantity from {position.quantity + position_trade.quantity} "
                    f"to {position.quantity}"
                )
                return True

        return False

    async def publish_trades(self, trades: List[Trade], now: datetime) -> bool:
        """Publish trades to all sinks"""
        publish_success = True

        for sink in self.sinks.values():
            if not sink.can_publish("trd"):
                continue

            if await sink.publish_trades(trades, now):
                logger.debug(f"Published {len(trades)} trades to {sink.sink_id}")
            else:
                logger.warning(f"Failed to publish trades to {sink.sink_id}")
                publish_success = False

        return publish_success

    async def _is_trade_published(self, trade: Trade) -> bool:
        """Check if a trade has already been published."""
        return await self.db.get_trade(trade.trade_id) is not None

    async def _save_trade(self, trade: Trade) -> None:
        """Save a trade to the database and update matching trade if exists."""
        try:
            # Convert domain trade to DB model
            db_trade = DBTrade.from_domain(trade)

            # Add new trade to session
            await self.db.save_trade(db_trade)

        except Exception as e:
            logger.error(f"Error saving trade: {str(e)}")
            raise e
