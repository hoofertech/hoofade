import logging
from datetime import datetime
from typing import Dict, List, Optional, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import default_timezone
from formatters.portfolio import PortfolioFormatter
from models.db_portfolio import DBPortfolio
from models.instrument import InstrumentType
from models.position import Position
from services.trade_processor import ProfitTaker
from sinks.base import MessageSink
from sources.base import TradeSource

logger = logging.getLogger(__name__)


class PositionService:
    def __init__(
        self,
        sources: Dict[str, TradeSource],
        sinks: Dict[str, MessageSink],
        db: AsyncSession,
    ):
        self.sources = sources
        self.sinks = sinks
        self.db = db
        self.portfolio_formatter = PortfolioFormatter()
        self.merged_positions: List[Position] = []

    async def publish_portfolio(self, positions: List[Position], timestamp: datetime):
        message = self.portfolio_formatter.format_portfolio(positions, timestamp)

        publish_success = True
        for sink in self.sinks.values():
            if sink.can_publish():
                if not await sink.publish(message):
                    publish_success = False
                    logger.error(f"Failed to publish portfolio to {sink.sink_id}")

        if publish_success:
            # Save portfolio post for all sources since it's consolidated
            await self._save_portfolio_post(timestamp)
            logger.info("Successfully published consolidated portfolio")

    async def should_post_portfolio(self, now: datetime) -> bool:
        """Check if we should post portfolio based on last post time"""
        last_post = await self._get_last_portfolio_post("all")
        logger.info(f"Last portfolio post: {last_post}")
        if last_post is None:
            return True

        last_post_day = last_post.date()
        current_day = now.date()
        logger.info(
            f"Last post day: {last_post_day}, current day: {current_day}: {'should post' if current_day > last_post_day else 'should not post'}"
        )
        return current_day > last_post_day

    async def _get_last_portfolio_post(self, source_id: str) -> Optional[datetime]:
        """Get the last portfolio post timestamp from DB"""
        try:
            query = select(DBPortfolio).where(DBPortfolio.source_id == source_id)
            result = await self.db.execute(query)
            record = result.scalar_one_or_none()

            if record is None:
                logger.warning(f"No portfolio post found for source_id: {source_id}")
                return None

            # Ensure the timestamp is timezone-aware
            if record.last_post.tzinfo is None:
                record.last_post = record.last_post.replace(tzinfo=default_timezone())

            return cast(datetime, record.last_post)
        except Exception as e:
            logger.error(f"Error getting last portfolio post: {str(e)}")
            return None

    async def _save_portfolio_post(self, timestamp: datetime):
        """Save or update the last portfolio post timestamp"""
        try:
            portfolio = DBPortfolio(last_post=timestamp, source_id="all")
            await self.db.merge(portfolio)
            await self.db.commit()
        except Exception as e:
            logger.error(f"Error saving portfolio post: {str(e)}")
            await self.db.rollback()

    async def apply_profit_taker(
        self, profit_taker: ProfitTaker, positions: List[Position]
    ) -> bool:
        """
        Apply a profit taker to the portfolio positions.
        Returns True if successfully applied.
        """
        new_positions = []
        # Find matching position
        for position in positions:
            if position.instrument == profit_taker.instrument:
                # Calculate the matched quantity
                matched_quantity = min(
                    abs(position.quantity),
                    min(
                        abs(profit_taker.buy_trade.quantity),
                        abs(profit_taker.sell_trade.quantity),
                    ),
                )

                # Update position quantity
                if position.is_short:
                    position.quantity += matched_quantity
                else:
                    position.quantity -= matched_quantity

                # If position is fully closed, remove it
                if position.quantity == 0:
                    positions.remove(position)
                    logger.info(f"Removed closed position for {position.instrument.symbol}")
                else:
                    new_positions.append(position)
                    logger.info(
                        f"Updated position quantity for {position.instrument.symbol} "
                        f"from {position.quantity + matched_quantity} to {position.quantity}"
                    )
                return True

        logger.debug(f"No matching position found for {profit_taker.instrument.symbol}")
        return False

    async def get_merged_positions(self) -> List[Position]:
        """Get merged positions from all sources."""
        positions_by_key = {}  # Store merged positions by instrument key

        # Get and merge positions from all sources
        for source_id, source in self.sources.items():
            logger.debug(f"Processing positions from {source.source_id}")
            for position in source.get_positions():
                # Skip positions with zero quantity
                if position.quantity == 0:
                    continue

                key = PositionService.get_position_key(position)
                if key in positions_by_key:
                    # Merge with existing position
                    existing = positions_by_key[key]
                    total_quantity = existing.quantity + position.quantity

                    # Skip if merged position would have zero quantity
                    if total_quantity == 0:
                        del positions_by_key[key]
                        continue

                    # Calculate weighted average cost basis
                    weighted_cost = (
                        existing.quantity * existing.cost_basis
                        + position.quantity * position.cost_basis
                    ) / total_quantity

                    positions_by_key[key] = Position(
                        instrument=position.instrument,
                        quantity=total_quantity,
                        cost_basis=weighted_cost,
                        market_price=position.market_price,  # Use latest mark price
                        report_time=position.report_time,
                    )
                else:
                    # New position
                    positions_by_key[key] = position

        return list(positions_by_key.values())

    @staticmethod
    def get_position_key(position: Position) -> str:
        """Generate a unique key for a position based on instrument details."""
        instrument = position.instrument
        if instrument.type == InstrumentType.OPTION and instrument.option_details:
            return (
                f"{instrument.symbol}_{instrument.option_details.expiry}_"
                f"{instrument.option_details.strike}_{instrument.option_details.option_type}"
            )
        return instrument.symbol
