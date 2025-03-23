import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

import aiosqlite

from config import get_db_url
from formatters.portfolio import PortfolioFormatter
from formatters.trade import TradeFormatter
from models.db_trade import DBTrade
from models.position import Position
from models.trade import Trade
from services.trade_processor import CombinedTrade, ProfitTaker
from utils.datetime_utils import format_datetime, parse_datetime

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages the database connection and provides basic operations"""

    def __init__(self, db_url: str):
        # Strip sqlite+aiosqlite:/// prefix if present
        self.db_path = db_url.replace("sqlite+aiosqlite:///", "")

    async def execute(self, query: str, params: Iterable[Any] | None = None) -> bool:
        """Execute a query that doesn't return results"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute(query, params)
                await conn.commit()
                return True
        except Exception as e:
            logger.error(f"Database error executing query: {e}", exc_info=True)
            return False

    async def fetch_one(self, query: str, params: Iterable[Any] | None = None) -> Optional[Dict]:
        """Execute a query and return a single row as dictionary"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(query, params)
                row = await cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Database error fetching row: {e}", exc_info=True)
            return None

    async def fetch_all(self, query: str, params: Iterable[Any] | None = None) -> List[Dict]:
        """Execute a query and return all rows as dictionaries"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(query, params)
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Database error fetching rows: {e}", exc_info=True)
            return []

    async def initialize(self):
        """Initialize all database tables"""
        try:
            # Create a direct connection without intermediary method calls
            async with aiosqlite.connect(self.db_path) as conn:
                # Create trades table
                await conn.execute("""
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
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_messages (
                        id TEXT PRIMARY KEY,
                        timestamp DATETIME NOT NULL,
                        message_metadata JSON NOT NULL,
                        source_id TEXT NOT NULL,
                        portfolio JSON NOT NULL
                    )
                """)

                # Create portfolio_posts table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_posts (
                        source_id TEXT PRIMARY KEY,
                        last_post DATETIME NOT NULL
                    )
                """)

                # Create trade_messages table
                await conn.execute("""
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

                # Add new table for bucket trades
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bucket_trades (
                        id TEXT PRIMARY KEY,
                        timestamp DATETIME NOT NULL,
                        granularity TEXT NOT NULL,
                        trades JSON NOT NULL,
                        UNIQUE(granularity)
                    )
                """)

                await conn.commit()
        except Exception as e:
            logger.error(f"Error initializing database: {e}", exc_info=True)
            raise

    async def close(self):
        """Close database connection (placeholder for future use)"""
        pass


class TradeRepository:
    """Repository for trade-related database operations"""

    def __init__(self, db_conn: DatabaseConnection):
        self.db = db_conn

    async def get_trade(self, trade_id: str) -> Optional[DBTrade]:
        """Get a trade by ID"""
        query = """
            SELECT trade_id, symbol, instrument_type, quantity, price,
                   side, currency, timestamp, source_id, option_type, strike, expiry
            FROM trades
            WHERE trade_id = ?
        """
        row = await self.db.fetch_one(query, (trade_id,))
        return DBTrade.from_dict(row) if row else None

    async def save_trade(self, trade_data: DBTrade) -> bool:
        """Save a trade to the database"""
        query = """
            INSERT INTO trades (
                trade_id, symbol, instrument_type, quantity, price,
                side, currency, timestamp, source_id,
                option_type, strike, expiry
            ) VALUES (
                :trade_id, :symbol, :instrument_type, :quantity, :price,
                :side, :currency, :timestamp, :source_id,
                :option_type, :strike, :expiry
            )
        """
        return await self.db.execute(query, trade_data.to_dict())

    async def get_trades_after(self, timestamp: datetime) -> List[Dict]:
        """Get all trades after a given timestamp"""
        query = """
            SELECT * FROM trades 
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        """
        return await self.db.fetch_all(query, (format_datetime(timestamp),))


class PortfolioRepository:
    """Repository for portfolio-related database operations"""

    def __init__(self, db_conn: DatabaseConnection):
        self.db = db_conn

    async def get_last_portfolio_post(self, source_id: str) -> Optional[datetime]:
        """Get the timestamp of the last portfolio post for a source"""
        query = "SELECT last_post FROM portfolio_posts WHERE source_id = ?"
        row = await self.db.fetch_one(query, (source_id,))
        return parse_datetime(row["last_post"]) if row else None

    async def save_portfolio_post(self, source_id: str, timestamp: datetime) -> bool:
        """Save a portfolio post timestamp"""
        query = """
            INSERT OR REPLACE INTO portfolio_posts (source_id, last_post)
            VALUES (?, ?)
        """
        return await self.db.execute(query, (source_id, format_datetime(timestamp)))

    async def remove_future_portfolio_messages(self, timestamp: datetime) -> bool:
        """Remove portfolio messages with timestamps in the future"""
        query = "DELETE FROM portfolio_messages WHERE timestamp > ?"
        return await self.db.execute(query, (format_datetime(timestamp),))

    async def save_portfolio_message(self, timestamp: datetime, positions: List[Position]) -> bool:
        """Save a portfolio message with positions"""
        query = """
            INSERT INTO portfolio_messages (
                id, timestamp, message_metadata,
                source_id, portfolio
            ) VALUES (?, ?, ?, ?, ?)
        """
        params = (
            format_datetime(timestamp),
            format_datetime(timestamp),
            json.dumps({"type": "pfl"}),
            "system",
            json.dumps([p.to_dict() for p in positions]),
        )
        return await self.db.execute(query, params)

    async def get_last_portfolio_message(
        self, before: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent portfolio message"""
        if before:
            query = """
                SELECT timestamp, portfolio
                FROM portfolio_messages
                WHERE timestamp < ?
                ORDER BY timestamp DESC
                LIMIT 1
            """
            return await self.db.fetch_one(query, (format_datetime(before),))
        else:
            query = """
                SELECT timestamp, portfolio
                FROM portfolio_messages
                ORDER BY timestamp DESC
                LIMIT 1
            """
            return await self.db.fetch_one(query)


class MessageRepository:
    """Repository for message-related database operations"""

    def __init__(self, db_conn: DatabaseConnection):
        self.db = db_conn
        self.trade_formatter = TradeFormatter()
        self.portfolio_formatter = PortfolioFormatter()

    async def get_messages(
        self,
        limit: int = 20,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        message_type: Optional[str] = None,
        granularity: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get messages with filtering options"""
        if message_type == "portfolio":
            rows = await self._get_portfolio_messages(limit, before, after)
            return [self._format_portfolio_message(row) for row in rows]
        elif message_type == "trade":
            rows = await self._get_trade_messages(limit, before, after, granularity)
            return [self._format_trade_message(row) for row in rows]
        else:
            # Get raw data with timestamps
            portfolio_rows = await self._get_portfolio_messages(limit, before, after)
            trade_rows = await self._get_trade_messages(limit, before, after, granularity)

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

    async def _get_portfolio_messages(
        self, limit: int, before: Optional[datetime], after: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """Get raw portfolio messages from the database"""
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

        rows = await self.db.fetch_all(query, tuple(params))

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
        limit: int,
        before: Optional[datetime],
        after: Optional[datetime],
        granularity: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Get raw trade messages from the database"""
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

        rows = await self.db.fetch_all(query, tuple(params))

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
        """Save a trade message to the database"""
        query = """
            INSERT INTO trade_messages (
                id, timestamp, granularity,
                message_metadata, source_id, trades, processed_trades
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            message["id"],
            message["timestamp"],
            message["granularity"],
            json.dumps(message["metadata"]),
            "system",
            json.dumps([t.to_dict() for t in message["trades"]]),
            json.dumps([pt.to_dict() for pt in message["processed_trades"]]),
        )
        return await self.db.execute(query, params)

    def _format_trade_message(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Format trade message from raw data"""
        processed_trades = row["processed_trades"]
        messages = self.trade_formatter.format_trades(
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
        message = self.portfolio_formatter.format_portfolio(portfolio, row["timestamp"])

        return {
            "id": row["id"],
            "content": message.content,
            "timestamp": format_datetime(row["timestamp"]),
            "metadata": row["metadata"],
            "message_type": "pfl",
        }

    def _process_trade_dict(
        self, trade_dict: Dict[str, Any]
    ) -> Union[Trade, ProfitTaker, CombinedTrade]:
        """Convert a trade dictionary to the appropriate trade object"""
        trade_type = trade_dict.get("ttype", "trade")

        if trade_type == "profit_taker":
            return ProfitTaker.from_dict(trade_dict)
        elif trade_type == "combined_trade":
            return CombinedTrade.from_dict(trade_dict)
        else:
            return Trade.from_dict(trade_dict)


class BucketTradeRepository:
    """Repository for bucket trade-related database operations"""

    def __init__(self, db_conn: DatabaseConnection):
        self.db = db_conn

    async def save_bucket_trades(
        self, granularity: str, trades: List[Trade], timestamp: datetime
    ) -> bool:
        """Save trades for a specific time bucket"""
        query = """
            INSERT OR REPLACE INTO bucket_trades (
                id, timestamp, granularity, trades
            ) VALUES (?, ?, ?, ?)
        """
        params = (
            f"bucket_{granularity}",
            format_datetime(timestamp),
            granularity,
            json.dumps([t.to_dict() for t in trades]),
        )
        return await self.db.execute(query, params)

    async def get_bucket_trades(self, granularity: str) -> List[Trade]:
        """Get trades for a specific time bucket"""
        query = "SELECT trades FROM bucket_trades WHERE granularity = ?"
        row = await self.db.fetch_one(query, (granularity,))

        if row:
            return sorted(
                [Trade.from_dict(t) for t in json.loads(row["trades"])],
                key=lambda x: x.timestamp,
                reverse=True,
            )
        return []


class Database:
    """Main database facade that provides access to all repositories"""

    def __init__(self, db_url: str):
        self.conn = DatabaseConnection(db_url)
        self.trade_repo = TradeRepository(self.conn)
        self.portfolio_repo = PortfolioRepository(self.conn)
        self.message_repo = MessageRepository(self.conn)
        self.bucket_repo = BucketTradeRepository(self.conn)

    async def initialize(self):
        """Initialize database tables"""
        await self.conn.initialize()

    async def close(self):
        """Close database connection"""
        await self.conn.close()

    # Trade-related methods
    async def get_trade(self, trade_id: str) -> Optional[DBTrade]:
        return await self.trade_repo.get_trade(trade_id)

    async def save_trade(self, trade_data: DBTrade) -> bool:
        return await self.trade_repo.save_trade(trade_data)

    async def get_trades_after(self, timestamp: datetime) -> List[Dict]:
        return await self.trade_repo.get_trades_after(timestamp)

    # Portfolio-related methods
    async def get_last_portfolio_post(self, source_id: str) -> Optional[datetime]:
        return await self.portfolio_repo.get_last_portfolio_post(source_id)

    async def save_portfolio_post(self, source_id: str, timestamp: datetime) -> bool:
        return await self.portfolio_repo.save_portfolio_post(source_id, timestamp)

    async def remove_future_portfolio_messages(self, timestamp: datetime) -> bool:
        return await self.portfolio_repo.remove_future_portfolio_messages(timestamp)

    async def save_portfolio_message(self, timestamp: datetime, positions: List[Position]) -> bool:
        return await self.portfolio_repo.save_portfolio_message(timestamp, positions)

    async def get_last_portfolio_message(
        self, before: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        return await self.portfolio_repo.get_last_portfolio_message(before)

    # Message-related methods
    async def get_messages(
        self,
        limit: int = 20,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        message_type: Optional[str] = None,
        granularity: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await self.message_repo.get_messages(
            limit, before, after, message_type, granularity
        )

    async def save_trade_message(self, message: Dict) -> bool:
        return await self.message_repo.save_trade_message(message)

    # Bucket trade-related methods
    async def save_bucket_trades(
        self, granularity: str, trades: List[Trade], timestamp: datetime
    ) -> bool:
        return await self.bucket_repo.save_bucket_trades(granularity, trades, timestamp)

    async def get_bucket_trades(self, granularity: str) -> List[Trade]:
        return await self.bucket_repo.get_bucket_trades(granularity)


async def create_db() -> Database:
    """Create database connection"""
    logger.info(f"Creating database connection: {get_db_url()}")
    db = Database(get_db_url())
    await db.initialize()
    return db
