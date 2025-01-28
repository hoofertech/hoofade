import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()


def get_source_configs() -> Dict[str, Dict[str, Any]]:
    """Get configurations for all trade sources"""
    sources = {}

    # IBKR Account 1
    if os.getenv("IBKR1_ENABLED", "false").lower() == "true":
        sources["ibkr1"] = {
            "type": "ibkr",
            "source_id": "ibkr-account1",
            "portfolio": {
                "token": os.getenv("IBKR1_FLEX_TOKEN"),
                "query_id": os.getenv("IBKR1_PORTFOLIO_QUERY_ID"),
            },
            "trades": {
                "token": os.getenv("IBKR1_FLEX_TOKEN"),
                "query_id": os.getenv("IBKR1_TRADES_QUERY_ID"),
            },
        }

    return sources


def get_sink_configs() -> Dict[str, Dict[str, Any]]:
    """Get configurations for all message sinks"""
    sinks = {}

    # Twitter
    if os.getenv("TWITTER_ENABLED", "false").lower() == "true":
        sinks["twitter"] = {
            "type": "twitter",
            "sink_id": "twitter-main",
            "bearer_token": os.getenv("TWITTER_BEARER_TOKEN"),
            "api_key": os.getenv("TWITTER_API_KEY"),
            "api_secret": os.getenv("TWITTER_API_SECRET"),
            "access_token": os.getenv("TWITTER_ACCESS_TOKEN"),
            "access_token_secret": os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
        }

    return sinks


def get_db_url() -> str:
    """Get database URL from environment"""
    return os.getenv("DATABASE_URL", "sqlite+aiosqlite:///trades.db")
