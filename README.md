# IBKR Trade Tweeter

A Python daemon that monitors IBKR trades using Flex Reports and automatically posts updates to X (formerly Twitter).

## Features

- Fetches trades from IBKR using Flex Reports
- Posts new trades to X (Twitter)
- Tracks matching trades and calculates P&L
- Rate-limited to respect X API limits
- Persistent storage of trades
- Error handling and logging

## IBKR Flex Report Setup

1. Log in to [IBKR Account Management](https://www.interactivebrokers.com/sso/Login)
2. Navigate to Reports > Flex Queries > Custom Flex Queries
3. Create two new Custom Flex Queries:

### Portfolio Query Setup
1. Click "Create New Flex Query"
2. Enter basic information:
   - Query Name: "Current Portfolio"
   - Type: "Model Portfolio"
3. Configure report settings:
   - Base Currency: Your account's base currency
   - Date Period: "Today"
   - Format: XML
4. Select sections and fields:
   - Expand "Positions" section
   - Required fields:
     - [x] Symbol
     - [x] Position
     - [x] Cost Basis
     - [x] Mark Price
5. Save the query and note down the Query ID

### Trades Query Setup
1. Click "Create New Flex Query"
2. Enter basic information:
   - Query Name: "Recent Trades"
   - Type: "Trade Confirmation Flex Query"
3. Configure report settings:
   - Base Currency: Your account's base currency
   - Date Period: "Last 24 Hours"
   - Format: XML
4. Select sections and fields:
   - Expand "Trades" section
   - Required fields:
     - [x] Symbol
     - [x] Date/Time
     - [x] Quantity
     - [x] Price
     - [x] Buy/Sell
     - [x] Execution ID
5. Save the query and note down the Query ID

### Get Flex Web Service Token
1. In Account Management, go to Reports > Settings
2. In the Flex Web Service section:
   - Click "Create" to generate a new token
   - Save this token securely

## Environment Setup

1. Clone the repository
2. Install Poetry if not already installed:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```
3. Install dependencies:
   ```bash
   poetry install
   ```
4. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
5. Edit `.env` and fill in:
   - Your IBKR Flex token
   - Both Query IDs from the steps above
   - Your Twitter API credentials

## Running Tests
