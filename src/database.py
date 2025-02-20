import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiosqlite

from config import (
    get_db_url,
)
from models.db_message import DBMessage
from models.db_trade import DBTrade
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

            # Create messages table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    message_metadata JSON NOT NULL,
                    source_id TEXT NOT NULL,
                    message_type TEXT NOT NULL
                )
            """)

            # Create portfolio_posts table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_posts (
                    source_id TEXT PRIMARY KEY,
                    last_post DATETIME NOT NULL
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

    async def save_message(self, message_data: DBMessage) -> bool:
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO messages (
                        id, content, timestamp, message_metadata,
                        source_id, message_type
                    ) VALUES (
                        :id, :content, :timestamp, :message_metadata,
                        :source_id, :message_type
                    )
                """,
                    message_data.to_dict(),
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving message: {e}", exc_info=True)
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
                    DELETE FROM messages
                    WHERE timestamp > ?
                    AND message_type = 'pfl'
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
    ) -> List[Dict[str, Any]]:
        """Get messages with filtering options"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row

                query = """
                    SELECT id, content, timestamp, message_metadata,
                           source_id, message_type
                    FROM messages
                    WHERE 1=1
                """
                params = []

                if before:
                    query += " AND timestamp < ?"
                    params.append(format_datetime(before))
                elif after:
                    query += " AND timestamp > ?"
                    params.append(format_datetime(after))

                if message_type and message_type != "all":
                    if message_type.lower() == "trade":
                        query += " AND message_type = 'trd'"
                    elif message_type.lower() == "pfl":
                        query += " AND message_type = 'pfl'"

                query += " ORDER BY timestamp DESC LIMIT ?"
                params.append(limit)

                cursor = await db.execute(query, params)
                rows = await cursor.fetchall()

                return [
                    {
                        "id": row["id"],
                        "content": row["content"],
                        "timestamp": parse_datetime(row["timestamp"]),
                        "metadata": json.loads(row["message_metadata"]),
                        "message_type": row["message_type"],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"Error fetching messages: {e}", exc_info=True)
            raise

    async def get_last_message(self) -> Optional[DBMessage]:
        """Get the last message"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row

                cursor = await db.execute(
                    """
                    SELECT id, content, timestamp, message_metadata,
                           source_id, message_type
                    FROM messages
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
                )
                row = await cursor.fetchone()
                return DBMessage.from_dict(dict(row)) if row else None
        except Exception as e:
            logger.error(f"Error fetching last message: {e}", exc_info=True)
            raise


async def create_db() -> Database:
    """Create database connection"""
    logger.info("Creating database connection: %s", get_db_url())
    db = Database(get_db_url())
    await db.initialize()
    return db
