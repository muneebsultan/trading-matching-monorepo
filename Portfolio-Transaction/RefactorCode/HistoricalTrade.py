import uuid
from Orchestration.MongoDBOperation import MongodbOperation

mo = MongodbOperation()


class HistoricalTradeExecutionManager:
    def __init__(self):
        pass

    async def _process_buy_or_sell(self, low_price, high_price, entry_price, trade_id, full_user_name, trade_date, quantity, side, high_volume, symbol, portfolio_id, user_id, order_type, stop_loss, take_profit, liquidation_price):
        try:
            if low_price <= entry_price <= high_price:
                handler = self._handle_buy if side == "buy" else self._handle_sell
                await handler(trade_id, full_user_name, trade_date, quantity, entry_price, high_volume, symbol, portfolio_id, user_id, order_type, stop_loss, take_profit, liquidation_price)
                return self._send_confirmation_embed(trade_date=trade_date, quantity=quantity, entry_price=entry_price, symbol=symbol)
            else:
                return(f"Trade Been Rejected due to Price not with in High: {high_price} and Low: {low_price}")
        except Exception as e:
            print(f"TradeExecutionManager-_process_buy_or_sell: {e}")

    async def _handle_buy(self, *args):
        await self._handle_trade(*args, side="buy")

    async def _handle_sell(self, *args):
        await self._handle_trade(*args, side="sell")

    async def _handle_trade(self, trade_id, full_user_name, trade_date, quantity, entry_price, high_volume, symbol, portfolio_id, user_id, order_type, stop_loss, take_profit, liquidation_price, side):
        try:
            await self._confirm_trade(trade_id, full_user_name, trade_date, quantity, entry_price, side, symbol, portfolio_id, user_id, take_profit, stop_loss, liquidation_price, order_type)
        except Exception as e:
            print(f"TradeExecutionManager-_handle_{side}: {e}")

    async def _confirm_trade(self, trade_id, full_user_name, trade_date, quantity, entry_price, side, symbol, portfolio_id, user_id, take_profit, stop_loss, liquidation_price, order_type):
        try:
            user_data = await mo.fetch_user_id(collection="users", discord_id=user_id)
            if user_data and '_id' in user_data[0]:
                _id = user_data[0]['_id']
            else:
                _id = user_id
            
            
            avg_transaction_trade_id = str(uuid.uuid4())
            
            await mo.upsert_average_transaction(
                collection_name="AverageTransactions",
                columns=["avg_transaction_trade_id", "user_id", "symbol", "quantity", "entry_price", "average_price", "portfolio_id"],
                values=[(
                    avg_transaction_trade_id,
                    _id,
                    symbol,
                    quantity,
                    entry_price,
                    entry_price,
                    portfolio_id
                )],
                conflict_columns=["symbol", "portfolio_id"],
                side=side
            )
            await self.record_order(trade_id=trade_id, full_user_name=full_user_name, trade_date=trade_date, quantity=quantity, entry_price=entry_price, side=side, symbol=symbol, order_type=order_type, user_id=_id, portfolio_id=portfolio_id, take_profit=take_profit, stop_loss=stop_loss, liquidation_price=liquidation_price)
        except Exception as e:
            print(f"TradeExecutionManager-_confirm_trade: {e}")
    
    async def record_order(self, trade_id, full_user_name, trade_date, quantity, entry_price, side, symbol, order_type, user_id, portfolio_id, stop_loss, take_profit, liquidation_price, tif_type = 'GTC'):
        try:
            document = {'trade_id': trade_id, 'username':full_user_name, 'symbol':symbol, 'side':side, 'type':order_type, 'Qty':int(quantity), 'limit_price':entry_price, 'entry_date':str(trade_date), 'status':'Filled', 'portfolio_id':portfolio_id, 'user_id': user_id, 'stop_loss':stop_loss, 'take_profit':take_profit, 'liquidation_price':liquidation_price, "tif_type": tif_type}
            await mo.insert(document=document, collection='order_trades', mongodb_client="Traderverse-Authentication")

            transaction_id = str(uuid.uuid4())
            document = {'transaction_id': transaction_id, 'username':full_user_name, 'symbol':symbol, 'quantity':int(quantity), 'entry_price':entry_price, 'entry_date':str(trade_date), 'trade_parameter':'Filled',  'type':order_type, 'side':side, 'portfolio_id':portfolio_id, 'user_id': user_id, 'trade_id':trade_id, 'stop_loss':stop_loss, 'take_profit':take_profit, 'liquidation_price':liquidation_price, "tif_type": tif_type}
            await mo.insert(document=document, collection='all_transactions', mongodb_client="Traderverse-Authentication")


        except Exception as e:
            print(f"LimitAdvanceTradeModal-_record_order: {e}")
    
    def _send_confirmation_embed(self, trade_date, quantity, entry_price, symbol):
        try:
            return (f"Your trade has been successfully recorded with the following details:\n **🔹 Symbol** {symbol}\n **📅 Trade Date** {trade_date} EST \n **🔢 Quantity** {quantity} \n **💰 Entry Price**  ${float(entry_price):,.2f}")

        except Exception as e:
            print(f"LimitAdvanceTradeModal-_send_confirmation_embed: {e}")