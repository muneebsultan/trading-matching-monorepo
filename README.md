# Portfolio Transactions Service

FastAPI-based service for placing and managing portfolio trades.  
It validates orders, stores trade/transaction records in MongoDB, fetches live prices from Redis, and forwards execution events to an external matching engine.

## Features

- Place limit and market orders
- Configure stop-loss and take-profit orders
- Modify and cancel pending trades
- Persist records in MongoDB (`order_trades`, `all_transactions`, `AverageTransactions`, `portfolio`, `trade_ledger`)
- Fetch live symbol prices from Redis
- Publish order actions to external matching engine APIs

## API Endpoints

Base URL (local): `http://localhost:port`

- `GET /` - health check
- `POST /LimitTrade` - place a limit trade
- `POST /MarketTrade` - place a market trade
- `POST /TakeProfitStopLoss` - create stop-loss or take-profit order
- `POST /ModifyTrade` - modify pending trade price
- `POST /CancelTrade` - cancel pending or partially-filled trade

## Request Models

### `POST /LimitTrade`

Required fields:
- `quantity` (float)
- `entry_price` (float)
- `side` (`buy` or `sell`)
- `user_id` (string)
- `portfolio_id` (string)
- `symbol` (string)
- `stockId` (string)

Optional fields:
- `stop_loss` (float)
- `take_profit` (float)
- `createdBy` (default: `Bot`)
- `asset_type` (default: `stock`)

### `POST /MarketTrade`

Required fields:
- `quantity` (float)
- `side` (`buy` or `sell`)
- `user_id` (string)
- `portfolio_id` (string)
- `symbol` (string)
- `stockId` (string)

Optional fields:
- `stop_loss` (float)
- `take_profit` (float)
- `createdBy` (default: `Bot`)
- `asset_type` (default: `stock`)

### `POST /TakeProfitStopLoss`

Required fields:
- `price` (float)
- `user_id` (string)
- `portfolio_id` (string)
- `symbol` (string)
- `order_type` (`stoploss` or `takeprofit`)
- `stockId` (string)

Optional fields:
- `createdBy` (default: `Bot`)

### `POST /ModifyTrade`

Required fields:
- `trade_id` (string)
- `update_price` (float)

### `POST /CancelTrade`

Required fields:
- `trade_id` (string)

## Project Structure

- `RefactorApp.py` - FastAPI entrypoint and route definitions
- `RefactorCode/RefactorTradeHub.py` - orchestration layer for trade lifecycle
- `RefactorCode/RefactorOMS.py` - order model, validation, leverage calculations, portfolio updates
- `RefactorCode/MatchingEngineAPI.py` - HTTP integration with matching engine APIs
- `RefactorCode/RedisData.py` - live market price fetch from Redis
- `RefactorCode/HistoricalTrade.py` - historical trade processing manager
- `Orchestration/MongoDBOperation.py` - MongoDB CRUD and aggregation helpers
- `logs/logger.py` - file logging + optional SendGrid email notification

## Environment Variables

Create a `.env` file in the repository root.

Required:

- `PRO_MONGODB_STRING` - MongoDB connection string
- `MATCHING_ENGINE_API` - endpoint for order placement / TP / SL events
- `MATCHING_ENGINE_MODIFY_ORDER` - endpoint for modify order action
- `MATCHING_ENGINE_CANCEL_ORDER` - endpoint for cancel order action
- `REDIS_HOST` - Redis host name or IP
- `REDIS_PORT` - Redis TCP port
- `TRADE_TABLE` - collection name used for trade records (for example `order_trades`)
- `TRANSACTION_TABLE` - collection name used for transaction records (for example `all_transactions`)
- `AVG_TRANSACTION_TABLE` - average positions collection name (for example `AverageTransactions`)
- `PROJECT_PATH` - absolute project path (used by `MongoDBOperation.py`)

Optional:

- `SENDGRID_API_KEY` - required only if email alerts from logger are enabled

## Local Setup (Python)

1. Create and activate virtual environment
   - Windows PowerShell:
     - `python -m venv .venv`
     - `.venv\Scripts\Activate.ps1`
2. Install dependencies
   - `pip install -r requirements.txt`
3. Add `.env` file with required variables
4. Run service
   - `python RefactorApp.py`
5. Test health endpoint
   - `GET http://localhost:8015/`

## Docker

### Build and run with Docker Compose

- `docker compose up --build`

This starts service `portfolio-app` and maps container port `8015` to local `8015`.

### Build and run with Docker only

- `docker build -t portfolio-transactions .`
- `docker run --env-file .env -p 8015:8015 portfolio-transactions`

## Notes and Operational Requirements

- The service expects live price data in Redis keys formatted as `<SYMBOL>_NASDAQ`.
- Order validation references `RefactorCode/stock_symbols.json`. Ensure this file exists and contains valid symbol objects (with `symbol` key).
- Matching engine endpoints must be reachable from this service runtime.
- MongoDB database default used in helpers is `Traderverse-Authentication`.

## Quick Example

Limit order request:

```json
{
  "quantity": 10,
  "entry_price": 190.5,
  "side": "buy",
  "stop_loss": 180,
  "take_profit": 210,
  "user_id": "user-123",
  "portfolio_id": "portfolio-456",
  "symbol": "AAPL",
  "createdBy": "Bot",
  "asset_type": "stock",
  "stockId": "AAPL"
}
```
