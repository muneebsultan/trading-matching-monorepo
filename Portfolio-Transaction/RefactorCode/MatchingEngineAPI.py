import requests
from __init__ import logger_object, matching_engine_api, matching_engine_modify_order, matching_engine_cancel_order

class MatchingEngine():
    def __init__(self, user_id, order_type, price, side, symbol, quantity, trade_id, portfolio_id, created_by, tif_type, stockId, asset_type, liquidation_price=None, stop_loss=None, take_profit=None, action = "placed"):
        self.user_id = str(user_id)
        self.action = action
        self.order_type = order_type
        self.side = side
        self.symbol = symbol
        self.quantity = quantity
        self.trade_id = trade_id #str(uuid.uuid4())
        self.portfolio_id = portfolio_id
        self.liquidation_price = liquidation_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.price = price
        self.tif_type = tif_type
        self.created_by = created_by
        self.stockId = stockId
        self.asset_type = asset_type

    def publish_order(self):
        try:  
            url = matching_engine_api
            headers = {
                "Content-Type": "application/json",  # Add if authentication is needed
            }
            data = {
            "trade_id": self.trade_id,
            "user_id": self.user_id,
            "action": self.action,
            "order_type": self.order_type,
            "side": self.side,
            "symbol": self.symbol, #"BTCUSD"
            "quantity": self.quantity,
            "portfolio_id": self.portfolio_id,
            "liquidation_price": self.liquidation_price,
            "take_profit": self.take_profit,
            "stop_loss": self.stop_loss,
            "price": self.price,
            "asset_type": self.asset_type,
            "createdBy": self.created_by,
            "stockId": self.stockId,
            "tif_type": self.tif_type

            }
            print(data)
            response = requests.post(url, json=data, headers=headers)

            if response.status_code == 200:
                logger_object['success'].log(f"Success: {response.json()}")
            else:
                logger_object['error'].log(f"Error: {response.status_code} {response.text}")
        
        except Exception as e:
            print(f"MatchingEngine-publish_order: {e}")
            logger_object['error'].log(f"MatchingEngine-publish_order: {e}")
    
    def take_profit_stop_loss(self, direction):
        try:
            url = matching_engine_api
            headers = {
                "Content-Type": "application/json",  # Add if authentication is needed
            }
            data = {
            "trade_id": self.trade_id,
            "user_id": self.user_id,
            "action": self.action,
            "order_type": self.order_type,
            "side": self.side,
            "symbol": self.symbol, #"BTCUSD"
            "quantity": self.quantity,
            "portfolio_id": self.portfolio_id,
            "liquidation_price": self.liquidation_price,
            "price": self.price,
            "direction": direction
            }

            response = requests.post(url, json=data, headers=headers)

            if response.status_code == 200:
                logger_object['success'].log(f"Success: {response.json()}")
            else:
                logger_object['error'].log(f"Error: {response.status_code} {response.text}")
        
        except Exception as e:
            print(f"MatchingEngine-publish_order: {e}")
            logger_object['error'].log(f"MatchingEngine-publish_order: {e}")

class MatchingEngineCancelModifyTrade():
    def __init__(self, trade_id, update_price = None, liquidation_price = None):
        self.trade_id = trade_id
        self.update_price = update_price
        self.liquidation_price = liquidation_price
    
    def ModifyTrade(self):
        try:
            url = matching_engine_modify_order
            headers = {
                "Content-Type": "application/json",  # Add if authentication is needed
            }
            data = {
                "trade_id": self.trade_id,
                "updated_price": self.update_price,
                "liquidation_price": self.liquidation_price
            }

            response = requests.post(url, json=data, headers=headers)

            if response.status_code == 200:
                logger_object['success'].log(f"Success: {response.json()}")
            else:
                logger_object['error'].log(f"Error: {response.status_code} {response.text}")
        
        except Exception as e:
            print(f"MatchingEngineCancelModifyTrade-ModifyTrade: {e}")
            logger_object['error'].log(f"MatchingEngineCancelModifyTrade-ModifyTrade: {e}")

    def CancelTrade(self):
        try:
            url = matching_engine_cancel_order
            headers = {
                "Content-Type": "application/json",  # Add if authentication is needed
            }
            data = {
                "trade_id": self.trade_id
            }

            response = requests.post(url, json=data, headers=headers)

            if response.status_code == 200:
                logger_object['success'].log(f"Success: {response.json()}")
            else:
                logger_object['error'].log(f"Error: {response.status_code} {response.text}")
        
        except Exception as e:
            print(f"MatchingEngineCancelModifyTrade-CancelTrade: {e}")
            logger_object['error'].log(f"MatchingEngineCancelModifyTrade-CancelTrade: {e}")
