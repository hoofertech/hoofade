import logging
import os
from typing import Any, Dict

import pytz
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def default_timezone():
    return pytz.timezone("America/New_York")


def get_source_configs() -> Dict[str, Dict[str, Any]]:
    """Get configurations for all trade sources"""
    sources = {}

    save_reports_dir = None
    if os.getenv("IBKR1_SAVE_REPORTS", "false").lower() == "true":
        save_reports_dir = "data/flex_reports"

    # IBKR Account 1
    if os.getenv("IBKR1_ENABLED", "false").lower() == "true":
        logger.info("IBKR1 source enabled")
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
            "save_dir": save_reports_dir,
        }

    # JSON Source (for testing)
    if os.getenv("IBKR0_JSON_SOURCE_ENABLED", "false").lower() == "true":
        logger.info("JSON source enabled")
        sources["json"] = {
            "type": "json",
            "source_id": "json-source",
            "data_dir": os.getenv("IBKR0_JSON_SOURCE_DATA_DIR", "data/flex_reports"),
        }

    return sources


def get_sink_configs() -> Dict[str, Dict[str, Any]]:
    """Get configurations for all message sinks"""
    sinks = {}

    # Twitter
    if os.getenv("TWITTER_ENABLED", "false").lower() == "true":
        logger.info("Twitter sink enabled")
        sinks["twitter"] = {
            "type": "twitter",
            "sink_id": "twitter-main",
            "bearer_token": os.getenv("TWITTER_BEARER_TOKEN"),
            "api_key": os.getenv("TWITTER_API_KEY"),
            "api_secret": os.getenv("TWITTER_API_SECRET"),
            "access_token": os.getenv("TWITTER_ACCESS_TOKEN"),
            "access_token_secret": os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
        }

    # CLI
    if (
        os.getenv("CLI_ENABLED", "false").lower() == "true"
        or os.getenv("TWITTER_ENABLED", "false").lower() != "true"
    ):
        logger.info("CLI sink enabled")
        sinks["cli"] = {
            "type": "cli",
            "sink_id": "cli-output",
        }

    # Add Database sink
    sinks["database"] = {
        "type": "database",
        "sink_id": "db-store",
    }

    return sinks


def get_db_url() -> str:
    """Get database URL from environment"""
    return os.getenv("DATABASE_URL", "sqlite+aiosqlite:///trades.db")


def get_web_config() -> Dict[str, Any]:
    """Get web server configuration"""
    return {
        "host": os.getenv("WEB_HOST", "0.0.0.0"),
        "port": int(os.getenv("WEB_PORT", "8000")),
        "log_level": os.getenv("WEB_LOG_LEVEL", "info"),
    }
