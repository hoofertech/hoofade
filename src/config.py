import os
from typing import Dict, Any
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()


def get_source_configs() -> Dict[str, Dict[str, Any]]:
    """Get configurations for all trade sources"""
    sources = {}

    # IBKR Account 1
    if os.getenv("IBKR1_ENABLED", "false").lower() == "true":
        sources["ibkr1"] = {
            "type": "ibkr",
            "source_id": "ibkr-account1",
            "host": os.getenv("IBKR1_HOST", "127.0.0.1"),
            "port": int(os.getenv("IBKR1_PORT", "7496")),
            "client_id": int(os.getenv("IBKR1_CLIENT_ID", "1")),
        }

    # IBKR Account 2
    if os.getenv("IBKR2_ENABLED", "false").lower() == "true":
        sources["ibkr2"] = {
            "type": "ibkr",
            "source_id": "ibkr-account2",
            "host": os.getenv("IBKR2_HOST", "127.0.0.1"),
            "port": int(os.getenv("IBKR2_PORT", "7497")),
            "client_id": int(os.getenv("IBKR2_CLIENT_ID", "2")),
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


def get_db_session():
    """Get database session"""
    database_url = os.getenv("DATABASE_URL", "sqlite:///trades.db")
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    return Session()
