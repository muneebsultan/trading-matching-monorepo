from datetime import datetime, date, timezone
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware 
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from pydantic import BaseModel, Field, validator
import uvicorn

from RefactorCode.RefactorTradeHub import TradeExecution, TakeProfitOrStopLoss, ModifyTrade, CancelTrade, HisToricalTradeExecution

class Limit(BaseModel):
    quantity: float
    entry_price: float
    side: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    user_id : str
    portfolio_id : str
    symbol : str
    createdBy : str = "Bot"
    asset_type : str = "stock"
    stockId : str

class Market(BaseModel):
    quantity: float
    side: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    user_id : str
    portfolio_id : str
    symbol : str
    createdBy : str = "Bot"
    asset_type : str = "stock"
    stockId : str

class TakeProfitOrStopLossParam(BaseModel):
    price: float
    user_id : str
    portfolio_id : str
    symbol : str
    order_type : str
    createdBy : str = "Bot"
    stockId : str

class ModifyTradeParam(BaseModel):
    trade_id : str
    update_price : float
# 
class CancelTradeParam(BaseModel):
    trade_id : str

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "200"}

@app.post("/LimitTrade")
async def run(input: Limit):
    try:
        trade_execution = TradeExecution()
        # Run inside an async function
        limit_trade = await trade_execution.show_trade_direction(
            user_id=input.user_id,
            portfolio_id=input.portfolio_id,
            symbol=input.symbol.upper(),
            quantity=input.quantity,
            price=input.entry_price,
            side=input.side.lower(),
            stop_loss=input.stop_loss,
            take_profit=input.take_profit,
            order_type='limit',
            createdBy=input.createdBy,
            asset_type = input.asset_type,
            stockId = input.stockId
        )

        return limit_trade
    
    except Exception as e:
        print(f"LimitTrade: {e}")
        return {"error": f"Failed to process trade: {str(e)}", "status": 0}  # ✅ Proper error response

@app.post("/MarketTrade")
async def run(input: Market):
    try:
        trade_execution = TradeExecution()
        # Run inside an async function
        limit_trade = await trade_execution.show_trade_direction(
            user_id=input.user_id,
            portfolio_id=input.portfolio_id,
            symbol=input.symbol.upper(),
            quantity=input.quantity,
            side=input.side.lower(),
            stop_loss=input.stop_loss,
            take_profit=input.take_profit,
            order_type='market',
            createdBy=input.createdBy,
            asset_type = input.asset_type,
            stockId = input.stockId
        )
        print(limit_trade)
        return limit_trade
    
    except Exception as e:
        print(f"LimitTrade: {e}")
        return {"error": f"Failed to process trade: {str(e)}", "status": 0}  # ✅ Proper error response

@app.post("/TakeProfitStopLoss")
async def run(input: TakeProfitOrStopLossParam):
    try:
        trade_execution = TakeProfitOrStopLoss()
        # Run inside an async function
        limit_trade = await trade_execution.operation_execute(
            user_id=input.user_id,
            portfolio_id=input.portfolio_id,
            symbol=input.symbol.upper(),
            order_type=input.order_type.lower(),
            price=input.price,
            stockId=input.stockId,
            createdBy=input.createdBy
        )
        return limit_trade
    
    except Exception as e:
        print(f"LimitTrade: {e}")
        return {"error": f"Failed to process trade: {str(e)}", "status": 0}  # ✅ Proper error response

@app.post("/ModifyTrade")
async def run(input: ModifyTradeParam):
    try:
        mt = ModifyTrade(trade_id=input.trade_id, update_price=input.update_price)
        # Run inside an async function

        result = await mt.operation_perform()

        return result
    
    except Exception as e:
        print(f"ModifyTrade: {e}")
        return {"error": f"Failed to ModifyTrade: {str(e)}", "status": 0}  # ✅ Proper error response

@app.post("/CancelTrade")
async def run(input: CancelTradeParam):
    try:
        mt = CancelTrade(trade_id=input.trade_id)
        # Run inside an async function

        result = await mt.operation_perform()

        return result
    
    except Exception as e:
        print(f"CancelTrade: {e}")
        return {"error": f"Failed to CancelTrade: {str(e)}", "status": 0}  # ✅ Proper error response

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8015)