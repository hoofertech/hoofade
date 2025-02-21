# Hoofer Trades (aka Hoofade, aka Trade Tweeter)

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
1. Login to IBKR Account
2. Navigate to Performance and Reports > Flex Queries
3. Select all your target accounts
4. On the "Activity Flex Query" section, click on "Create" (like a + sign)
5. Enter basic information:
   - Query Name: "Current Portfolio"
6. On the "sections" section:
   - Click on "hoofade-positions"
      - Required fields:
         - [x] Account ID
         - [x] Currency
         - [x] Symbol
         - [x] Listing Exchange
         - [x] Underlying Symbol
         - [x] Expiry
         - [x] Put/Call
         - [x] Quantity
         - [x] Mark Price
         - [x] Cost Basis Price
         - [x] Strike
   - Click on "Save"
7. On the "Delivery Configuration" section:
   - Make sure all your target accounts are selected
   - Models: "Optional"
   - Format: "XML"
   - Period: "Last Business Day"
8. On the "General Configuration" section:
   - Date Format: yyyyMMdd
   - Time Format: HHmmss
   - Date/Time Separator: "; (semicolon)"
   - Profit and Loss: "Default"
   - Include Canceled Trades? "No"
   - Include Currency Rates? "No"
   - Include Audit Trail Fields? "No"
   - Display Account Alias in Place of Account ID? "No"
   - Breakout by Day? "No"
9. Save the query and note down the Query ID

### Trades Query Setup
1. Login to IBKR Account
2. Navigate to Performance and Reports > Flex Queries
3. Select all your target accounts
4. On the "Trade Confirmation Flex Query" section, click on "Create" (like a + sign)
5. Enter basic information:
   - Query Name: "hoofade-recent"
6. On the "sections" section:
   - Click on "Trade Confirmation"
   - Required fields:
      - [x] Account ID
      - [x] Currency
      - [x] Symbol
      - [x] Listing Exchange
      - [x] Expiry
      - [x] Put/Call
      - [x] Trade ID
      - [x] Date/Time
      - [x] Buy/Sell
      - [x] Quantity
      - [x] Price
      - [x] Strike
      - [x] Underlying Symbol
      - [x] Commission
      - [x] Commission Currency
   - Click on "Save"
7. On the "Delivery Configuration" section:
   - Make sure all your target accounts are selected
   - Models: "Optional"
   - Format: "XML"
   - Period: "Today"
8. On the "General Configuration" section:
   - Date Format: yyyyMMdd
   - Time Format: HHmmss
   - Date/Time Separator: "; (semicolon)"
   - Include Canceled Trades? "No"
   - Include Audit Trail Fields? "No"
   - Display Account Alias in Place of Account ID? "No"
9. Save the query and note down the Query ID

### Get Flex Web Service Token
1. In Account Management, go to Reports > Settings
2. In the Flex Web Service section:
   - Click "Create" to generate a new token
   - Save this token securely

### X (Twitter) API Setup
1. Go to the [X Developer Portal](https://developer.x.com)
2. Sign in with your X account
3. Create a new Project/App:
   - Click "Create Project"
   - Name your project
   - Select "Production" access
   - In "User authentication setup":
     - Enable OAuth 1.0a
     - Type of App: select "Web App, Automated App or Bot"
     - App permissions: select "Read and Write"
     - Callback URI / Website URL: can be left blank for bot usage
4. Get your API Keys:
   - From your app's dashboard, go to "Keys and tokens"
   - Save these credentials:
     - API Key (Consumer Key)
     - API Key Secret (Consumer Secret)
     - Access Token
     - Access Token Secret
5. Add to your .env file:
   ```plaintext
   TWITTER_API_KEY=your_api_key
   TWITTER_API_SECRET=your_api_secret
   TWITTER_BEARER_TOKEN=your_bearer_token
   TWITTER_ACCESS_TOKEN=your_access_token
   TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret
   ```

Note: If you change app permissions after generating tokens, you'll need to regenerate your Access Token and Secret.

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
