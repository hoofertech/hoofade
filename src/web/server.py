from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy import select
from typing import Optional, AsyncGenerator
from datetime import datetime
from models.db_message import DBMessage
import logging
import os

logger = logging.getLogger(__name__)

app = FastAPI()

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Will be set when initializing the app
async_session_maker: Optional[async_sessionmaker[AsyncSession]] = None


def init_app(session_maker: async_sessionmaker[AsyncSession]):
    global async_session_maker
    async_session_maker = session_maker


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if async_session_maker is None:
        raise RuntimeError("Database session maker not initialized")
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


@app.get("/")
async def read_root():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/api/messages")
async def get_messages(
    session: AsyncSession = Depends(get_session),
    limit: int = 20,
    before: Optional[datetime] = None,
    type: Optional[str] = None,
):
    try:
        query = select(DBMessage).order_by(DBMessage.timestamp.desc())

        if before:
            query = query.where(DBMessage.timestamp < before)

        if type and type != "all":
            # Match the exact message_type from the database
            if type.lower() == "trade":
                query = query.where(DBMessage.message_type == "trade_batch")
            elif type.lower() == "portfolio":
                query = query.where(DBMessage.message_type == "portfolio")

        query = query.limit(limit)
        result = await session.execute(query)
        messages = result.scalars().all()

        return {
            "messages": [
                {
                    "id": msg.id,
                    "content": msg.content,
                    "timestamp": msg.timestamp,
                    "metadata": msg.message_metadata,
                    "message_type": msg.message_type,
                }
                for msg in messages
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching messages: {str(e)}")
        return {"error": str(e)}
