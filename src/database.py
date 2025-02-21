import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import aiosqlite

from config import (
    get_db_url,
)
from formatters.portfolio import PortfolioFormatter
from formatters.trade import TradeFormatter
from models.db_trade import DBTrade
from models.position import Position
from models.trade import Trade
from services.trade_processor import (
    CombinedTrade,
    ProfitTaker,
)
from utils.datetime_utils import format_datetime, parse_datetime

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_url: str):
        # Strip sqlite+aiosqlite:/// prefix if present
        self.db_path = db_url.replace("sqlite+aiosqlite:///", "")

    async def initialize(self):
        """Initialize database tables"""
        async with aiosqlite.connect(self.db_path) as db:
            # Create trades table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    instrument_type TEXT NOT NULL,
                    quantity DECIMAL NOT NULL,
                    price DECIMAL NOT NULL,
                    side TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    source_id TEXT NOT NULL,
                    option_type TEXT,
                    strike DECIMAL,
                    expiry DATE
                )
            """)

            # Create portfolio messages table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_messages (
                    id TEXT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    message_metadata JSON NOT NULL,
                    source_id TEXT NOT NULL,
                    portfolio JSON NOT NULL
                )
            """)

            # Create portfolio_posts table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_posts (
                    source_id TEXT PRIMARY KEY,
                    last_post DATETIME NOT NULL
                )
            """)

            # Create trade_messages table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trade_messages (
                    id TEXT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    granularity TEXT NOT NULL,
                    message_metadata JSON NOT NULL,
                    source_id TEXT NOT NULL,
                    trades JSON NOT NULL,
                    processed_trades JSON NOT NULL
                )
            """)

            await db.commit()

    async def get_trade(self, trade_id: str) -> Optional[DBTrade]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT trade_id, symbol, instrument_type, quantity, price,
                       side, currency, timestamp, source_id, option_type, strike, expiry
                FROM trades
                WHERE trade_id = ?
            """,
                (trade_id,),
            )
            row = await cursor.fetchone()
            return DBTrade.from_dict(dict(row)) if row else None

    async def save_trade(self, trade_data: DBTrade) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO trades (
                        trade_id, symbol, instrument_type, quantity, price,
                        side, currency, timestamp, source_id,
                        option_type, strike, expiry
                    ) VALUES (
                        :trade_id, :symbol, :instrument_type, :quantity, :price,
                        :side, :currency, :timestamp, :source_id,
                        :option_type, :strike, :expiry
                    )
                """,
                    trade_data.to_dict(),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving trade: {e}", exc_info=True)
            return False

    async def get_last_portfolio_post(self, source_id: str) -> Optional[datetime]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT last_post FROM portfolio_posts WHERE source_id = ?", (source_id,)
            )
            row = await cursor.fetchone()
            return parse_datetime(row[0]) if row else None

    async def save_portfolio_post(self, source_id: str, timestamp: datetime) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO portfolio_posts (source_id, last_post)
                    VALUES (?, ?)
                """,
                    (source_id, format_datetime(timestamp)),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving portfolio post: {e}", exc_info=True)
            return False

    async def remove_future_portfolio_messages(self, timestamp: datetime) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    DELETE FROM portfolio_messages
                    WHERE timestamp > ?
                """,
                    (format_datetime(timestamp),),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error removing future portfolio messages: {e}", exc_info=True)
            return False

    async def close(self):
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.close()
        except Exception as e:
            logger.error(f"Error closing database: {e}", exc_info=True)

    async def get_messages(
        self,
        limit: int = 20,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        message_type: Optional[str] = None,
        granularity: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get messages with filtering options"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row

                if message_type == "portfolio":
                    rows = await self._get_portfolio_messages(db, limit, before, after)
                    return [self._format_portfolio_message(row) for row in rows]
                elif message_type == "trade":
                    rows = await self._get_trade_messages(db, limit, before, after, granularity)
                    return [self._format_trade_message(row) for row in rows]
                else:
                    # Get raw data with timestamps
                    portfolio_rows = await self._get_portfolio_messages(db, limit, before, after)
                    trade_rows = await self._get_trade_messages(
                        db, limit, before, after, granularity
                    )

                    # Combine and sort raw rows
                    all_rows = []
                    all_rows.extend((row, "portfolio") for row in portfolio_rows)
                    all_rows.extend((row, "trade") for row in trade_rows)

                    # Sort by timestamp and limit
                    all_rows.sort(key=lambda x: x[0]["timestamp"], reverse=True)
                    limited_rows = all_rows[:limit]

                    # Format only the limited rows
                    return [
                        self._format_portfolio_message(row)
                        if msg_type == "portfolio"
                        else self._format_trade_message(row)
                        for row, msg_type in limited_rows
                    ]

        except Exception as e:
            logger.error(f"Error fetching messages: {e}", exc_info=True)
            raise

    async def _get_portfolio_messages(
        self,
        db: aiosqlite.Connection,
        limit: int,
        before: Optional[datetime],
        after: Optional[datetime],
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT id, timestamp, message_metadata,
                   source_id, portfolio
            FROM portfolio_messages
            WHERE 1=1
        """
        params = []

        if before:
            query += " AND timestamp < ?"
            params.append(format_datetime(before))
        elif after:
            query += " AND timestamp > ?"
            params.append(format_datetime(after))

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()

        return [
            {
                "id": row["id"],
                "timestamp": parse_datetime(row["timestamp"]),
                "metadata": json.loads(row["message_metadata"]),
                "message_type": "pfl",
                "portfolio": json.loads(row["portfolio"]),
            }
            for row in rows
        ]

    async def _get_trade_messages(
        self,
        db: aiosqlite.Connection,
        limit: int,
        before: Optional[datetime],
        after: Optional[datetime],
        granularity: Optional[str],
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT id, timestamp, message_metadata,
                   source_id, granularity, trades, processed_trades
            FROM trade_messages
            WHERE 1=1
        """
        params = []

        if before:
            query += " AND timestamp < ?"
            params.append(format_datetime(before))
        elif after:
            query += " AND timestamp > ?"
            params.append(format_datetime(after))

        if granularity:
            query += " AND granularity = ?"
            params.append(granularity)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()

        return [
            {
                "id": row["id"],
                "timestamp": parse_datetime(row["timestamp"]),
                "metadata": json.loads(row["message_metadata"]),
                "message_type": "trd",
                "granularity": row["granularity"],
                "trades": json.loads(row["trades"]),
                "processed_trades": json.loads(row["processed_trades"]),
            }
            for row in rows
        ]

    async def save_trade_message(self, message: Dict) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO trade_messages (
                        id, timestamp, granularity,
                        message_metadata, source_id, trades, processed_trades
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message["id"],
                        message["timestamp"],
                        message["granularity"],
                        json.dumps(message["metadata"]),
                        "system",
                        json.dumps([t.to_dict() for t in message["trades"]]),
                        json.dumps([pt.to_dict() for pt in message["processed_trades"]]),
                    ),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving trade message: {e}", exc_info=True)
            return False

    async def save_portfolio_message(self, timestamp: datetime, positions: List[Position]) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO portfolio_messages (
                        id, timestamp, message_metadata,
                        source_id, portfolio
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        format_datetime(timestamp),
                        format_datetime(timestamp),
                        json.dumps({"type": "pfl"}),
                        "system",
                        json.dumps([p.to_dict() for p in positions]),
                    ),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving portfolio message: {e}")
            return False

    def _format_trade_message(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Format trade message from raw data"""
        processed_trades = row["processed_trades"]
        formatter = TradeFormatter()
        messages = formatter.format_trades(
            [self._process_trade_dict(pt) for pt in processed_trades]
        )

        content = [
            f"Trades for {row['granularity']} interval",
            "",  # Empty line after header
        ]
        for msg in messages:
            content.append(msg.content)

        return {
            "id": row["id"],
            "content": "\n".join(content),
            "timestamp": format_datetime(row["timestamp"]),
            "metadata": row["metadata"],
            "message_type": "trd",
            "granularity": row["granularity"],
        }

    def _format_portfolio_message(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Format portfolio message from raw data"""
        portfolio = [Position.from_dict(p) for p in row["portfolio"]]
        formatter = PortfolioFormatter()
        message = formatter.format_portfolio(portfolio, row["timestamp"])

        return {
            "id": row["id"],
            "content": message.content,
            "timestamp": format_datetime(row["timestamp"]),
            "metadata": row["metadata"],
            "message_type": "pfl",
        }

    async def get_last_portfolio_message(self, before: datetime) -> Optional[Dict[str, Any]]:
        """Get the most recent portfolio message"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT timestamp, portfolio
                    FROM portfolio_messages
                    WHERE timestamp < ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (format_datetime(before),),
                )
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting last portfolio message: {e}")
            return None

    def _sort_trades_by_timestamp(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def get_timestamp(trade: Dict[str, Any]) -> datetime:
            ts = trade.get("timestamp")
            if isinstance(ts, str):
                return parse_datetime(ts)
            elif isinstance(ts, datetime):
                return ts
            return datetime.min

        return sorted(trades, key=get_timestamp)

    def _process_trade_dict(
        self, trade_dict: Dict[str, Any]
    ) -> Union[Trade, ProfitTaker, CombinedTrade]:
        trade_type = trade_dict.get("ttype", "trade")

        if trade_type == "profit_taker":
            return ProfitTaker.from_dict(trade_dict)
        elif trade_type == "combined_trade":
            return CombinedTrade.from_dict(trade_dict)
        else:
            return Trade.from_dict(trade_dict)

    async def get_trades_after(self, timestamp: datetime) -> List[Dict]:
        """Get all trades after a given timestamp"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    """
                    SELECT * FROM trades 
                    WHERE timestamp >= ?
                    ORDER BY timestamp ASC
                    """,
                    (format_datetime(timestamp),),
                )
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting trades after timestamp: {e}")
            return []


async def create_db() -> Database:
    """Create database connection"""
    logger.info("Creating database connection: %s", get_db_url())
    db = Database(get_db_url())
    await db.initialize()
    return db
