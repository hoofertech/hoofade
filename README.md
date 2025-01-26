# IBKR Trade Tweeter

A Python daemon that monitors IBKR trades and automatically posts updates to X (formerly Twitter).

## Features

- Connects to Interactive Brokers (IBKR) every 15 minutes
- Posts new trades to X (Twitter)
- Tracks matching trades and calculates P&L
- Rate-limited to respect X API limits
- Persistent storage of trades
- Error handling and logging

## Setup

1. Clone the repository
2. Create virtual environment:
   ```bash
   python -m venv env
   source env/bin/activate  # or `env\Scripts\activate` on Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in your credentials
5. Make sure IBKR Trader Workstation (TWS) or IB Gateway is running
6. Run the tests:
   ```bash
   pytest
   ```
7. Start the daemon:
   ```bash
   python -m src.main
   ```

## Configuration

The following environment variables are required:

- `IBKR_HOST`: IBKR TWS/Gateway host (default: 127.0.0.1)
- `IBKR_PORT`: IBKR TWS/Gateway port (default: 7496)
- `IBKR_CLIENT_ID`: IBKR client ID (default: 1)
- `TWITTER_BEARER_TOKEN`: X API bearer token
- `TWITTER_API_KEY`: X API key
- `TWITTER_API_SECRET`: X API secret
- `TWITTER_ACCESS_TOKEN`: X access token
- `TWITTER_ACCESS_TOKEN_SECRET`: X access token secret
- `DATABASE_URL`: SQLAlchemy database URL (default: sqlite:///trades.db)

## Rate Limits

- Maximum 12 tweets per day
- Maximum 500 tweets per month

## License

MIT
