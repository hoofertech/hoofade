import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from database import Database

logger = logging.getLogger(__name__)

app = FastAPI()

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Will be set when initializing the app
db: Optional[Database] = None


def init_app(database: Database):
    global db
    db = database


@app.get("/")
async def read_root():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/api/messages")
async def get_messages(
    limit: int = 20,
    before: Optional[datetime] = None,
    after: Optional[datetime] = None,
    type: Optional[str] = None,
):
    if db is None:
        raise RuntimeError("Database not initialized")

    try:
        messages = await db.get_messages(
            limit=limit,
            before=before,
            after=after,
            message_type=type,
        )
        return {"messages": messages}
    except Exception as e:
        logger.error(f"Error fetching messages: {str(e)}")
        return {"error": str(e)}
