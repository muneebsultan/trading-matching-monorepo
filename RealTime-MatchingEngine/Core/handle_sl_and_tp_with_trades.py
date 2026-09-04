import os
import uuid
from bson import ObjectId 
from datetime import datetime, timezone
from Core.__init__ import logger_object  # Custom logger import

from dotenv import load_dotenv
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

class SlAndTpWithTrades():
    def __init__(self, stoploss, takeprofit, symbol, user_id, portfolio_id, stockId, createdBy, asset_type):
        self.topic = "Stocks-Order-Placed"
        self.symbol = symbol
        self.user_id = user_id
        self.portfolio_id = portfolio_id
        self.stockId = stockId
        self.takeprofit = takeprofit
        self.stoploss = stoploss
        self.asset_type = asset_type
        self.createdBy = createdBy

    def process_sl_and_tp(self, insert):
        logger_object["success"].log(f"self.stoploss {self.stoploss}")  
        if self.stoploss is not None:
            logger_object["success"].log(f"andar self.stoploss {self.stoploss}")  
            price = self.stoploss
            order_type = "stoploss"
            
            self.add_record_in_trade_and_transaction(price=price, order_type=order_type, insert=insert)
            
        if self.takeprofit is not None:
            price = self.takeprofit
            order_type = "takeprofit"
            self.add_record_in_trade_and_transaction(price=price, order_type=order_type, insert=insert)


    def add_record_in_trade_and_transaction(self, price, order_type, insert):
        try:
            trade_id = str(uuid.uuid4()
                           )
            order_place_document_trades = {
                'trade_id': trade_id,
                'symbol': self.symbol,
                'remaining_quantity': None,
                'status': 'Pending',
                'entry_price': price,
                'price': price,
                'portfolio_id': self.portfolio_id,
                'user_id': self.user_id,
                'order_type': order_type,
                'entry_date': datetime.now(timezone.utc),
                'createdBy': self.createdBy,
                'asset_type': self.asset_type
            }

            insert(document=order_place_document_trades, collection=trade_table, mongodb_client="Traderverse-Authentication")
            logger_object["success"].log(f"Success: successfully add document in trades table with trade_id: {trade_id}")

            order_place_document_transaction = {
                'transaction_id': ObjectId(), 
                'trade_id': trade_id,
                'symbol': self.symbol,
                'trade_parameter': 'Pending',
                'entry_price': price,
                'price': price,
                'portfolio_id': self.portfolio_id,
                'user_id': self.user_id,
                'order_type': order_type,
                'entry_date': datetime.now(timezone.utc),
                'createdBy': self.createdBy,
                'asset_type': self.asset_type
            }

            insert(document=order_place_document_transaction, collection=transaction_table, mongodb_client="Traderverse-Authentication")
            logger_object["success"].log(f"Success: successfully add document in transaction table with trade_id: {trade_id}")
        
        except Exception as e:
            logger_object["error"].log(f"Error: add_record_in_trade_and_transaction: Face error {e}")
