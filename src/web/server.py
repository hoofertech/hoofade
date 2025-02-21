import logging
import os
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from database import Database
from utils.datetime_utils import parse_datetime

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
    limit: int = Query(default=20, ge=1, le=100),
    before: Optional[str] = None,
    after: Optional[str] = None,
    type: Optional[str] = None,
    granularity: Optional[str] = None,
):
    logger.info(
        f"Getting messages with limit: {limit}, before: {before}, after: {after}, type: {type}"
    )
    if db is None:
        raise RuntimeError("Database not initialized")

    try:
        # Parse datetime parameters
        before_dt = parse_datetime(before) if before else None
        after_dt = parse_datetime(after) if after else None

        messages = await db.get_messages(
            limit=limit,
            before=before_dt,
            after=after_dt,
            message_type=type,
            granularity=granularity,
        )

        return {"messages": messages}
    except ValueError as e:
        logger.error(f"Datetime parsing error: {str(e)}", exc_info=True)
        return {"error": f"Invalid datetime format: {str(e)}"}, 422
    except Exception as e:
        logger.error(f"Error fetching messages: {str(e)}", exc_info=True)
        return {"error": str(e)}, 500
