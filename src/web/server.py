import logging
import os
from typing import Optional
from datetime import datetime
import json

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from database import Database
from utils.datetime_utils import parse_datetime, format_datetime
from services.trade_bucket_manager import TradeBucketManager
from services.trade_processor import TradeProcessor
from formatters.trade import TradeFormatter
from models.position import Position

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


@app.get("/api/in-progress/{granularity}")
async def get_in_progress_message(granularity: str):
    try:
        bucket_trades = await db.get_bucket_trades(granularity)
        if not bucket_trades:
            return {"message": None}
            
        now = datetime.now()
        start_time = TradeBucketManager.round_time_down(
            bucket_trades[0].timestamp, 
            TradeBucketManager.intervals[granularity]
        )
        
        # Load latest portfolio for accurate trade processing
        latest_portfolio = await db.get_last_portfolio_message()
        positions = []
        if latest_portfolio:
            portfolio_json = json.loads(latest_portfolio["portfolio"])
            positions = [Position.from_dict(p) for p in portfolio_json]
        
        # Process trades with actual portfolio state
        processor = TradeProcessor(positions)
        processed_results, _ = processor.process_trades(bucket_trades)
        
        formatter = TradeFormatter()
        trade_messages = formatter.format_trades(processed_results)
        
        content = [
            f"🔄 In-Progress Trades ({granularity})",
            "",  # Empty line
        ]
        
        for msg in trade_messages:
            content.append(msg.content)
            
        return {
            "message": {
                "id": f"in_progress_{granularity}",
                "timestamp": format_datetime(now),
                "content": "\n".join(content),
                "metadata": {
                    "status": "in_progress",
                    "granularity": granularity,
                    "start_time": format_datetime(start_time)
                },
                "message_type": "trade",
            }
        }
    except Exception as e:
        logger.error(f"Error getting in-progress message: {str(e)}", exc_info=True)
        return {"error": str(e)}, 500
