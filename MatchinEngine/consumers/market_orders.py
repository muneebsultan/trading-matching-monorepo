import os
import asyncio
from datetime import datetime, timezone

from Core.__init__ import logger_object
from Core.general import DatabaseManager 
from Core.nats_price_service import NATSPriceService 
from Core.handle_portfolio_balance_and_fund import HandlePortfolioBalanceAndFund
from Core.recalculate_metrics_for_portfolio import AlertForRecalculatePortfolio


from dotenv import load_dotenv
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")
        
alert_for_recalculating = AlertForRecalculatePortfolio()

class StockOrderProcessor:
    def __init__(self):
        """
        Initialize Redis, DatabaseManager, and Logger.
        """

        # Initialize DatabaseManager
        self.db_manager = DatabaseManager()

        # Configure Logging
        self.logger_object = logger_object

    def process_market_order(self, event, order_type):
        """
        Process Market Buy/Sell Orders
        """
        try:
            price_service = NATSPriceService()

            trade_id = event.get("trade_id")
            symbol = event.get("symbol", "").upper()
            quantity = event.get("quantity")
            stoploss = event.get("stop_loss")
            takeprofit = event.get("take_profit")
            stockId = event.get("stockId")
            # self.logger_object['info'].log(f"🔄 Processing Market {order_type.capitalize()} Order {trade_id} for {symbol} - Quantity: {quantity}")

            # Check if order exists in Mongo
            order = self.db_manager.get_order_from_mongo(trade_id)
            self.logger_object['success'].log(f"🔄 Successfully retrieved order for trade_id: {trade_id}") 
            if not order:
                self.db_manager.save_first_record(event)
                # self.logger_object['info'].log(f"🔄 Order not found in DB. Creating a new order for trade_id: {trade_id}")

            previous_data = None
            total_executed_quantity = 0
            total_executed_value = 0
            event["entry_date"] = datetime.now(timezone.utc)
            entry_time_str = event["entry_date"].isoformat()
            first_run = True

            self.db_manager.update_all_transaction_records(trade_id, event,status="Pending")
            self.logger_object['success'].log(f"🔄 Successfully update transactions for trade_id: {trade_id}, event : {event} and Status: Pending") 

            while True:
                try:
                    executed_quantity = 0

                    order_data = self.db_manager.get_order_from_mongo(trade_id)
                    self.logger_object['success'].log(f"🔄 Successfully retrieved order order_data : {order_data}")

                    remaining_quantity = order_data.get("remaining_quantity")
                    user_requested_quantity  = order_data.get("quantity")
                    user_id = order_data.get("user_id")
                    portfolio_id = order_data.get("portfolio_id")
                    asset_type = order_data.get("asset_type")
                    createdBy =  order_data.get("createdBy"),
                    
                    remaining_quantity = float(remaining_quantity)
                    if remaining_quantity <= 0:
                        self.logger_object['info'].log(f"✅ Order {trade_id} fully executed.")
                        break
            
                    if first_run:
                        stock_data = price_service.get_latest_stock_price(symbol.upper(), user_trade_time=entry_time_str)
                        if stock_data:
                            first_run = False
                            self.db_manager.insert_all_transaction_records(trade_id, event,status="Partial Filled")
                            # alert_response = alert_for_recalculating.alert_response(portfolioId=portfolio_id)

                            try:
                                loop = asyncio.get_event_loop()
                                if loop.is_running():
                                    # Already in an event loop; schedule as a task
                                    asyncio.create_task(alert_for_recalculating.alert_response(portfolioId=portfolio_id))
                                else:
                                    # No running loop; run it directly
                                    loop.run_until_complete(alert_for_recalculating.alert_response(portfolioId=portfolio_id))
                            except RuntimeError as e:
                                # No event loop available at all
                                asyncio.run(alert_for_recalculating.alert_response(portfolioId=portfolio_id))
                            except Exception as e:
                                self.logger_object['error'].log(f"❌ Error running Recalculating Market Partial Filled: {e}")

                    else:
                        stock_data = price_service.get_latest_stock_price(symbol.upper(), user_trade_time=None)

                    if not stock_data:
                        self.logger_object['info'].log(f"❌ Order {trade_id} No market price data for {symbol}")
                        continue  
                    
                    if stock_data:
                        self.logger_object['info'].log(f"New stock price {stock_data}")
                        
                    timestamp, _, ask_price, ask_volume, bid_price, bid_volume = stock_data
                    price, volume = (ask_price, ask_volume) if order_type == "buy" else (bid_price, bid_volume)

                    if price is None or volume == 0:
                        self.logger_object['info'].log(f"🚫 No valid price/volume for {symbol}")
                        continue

                    # Execute order
                    executed_quantity = min(remaining_quantity, volume)
                    remaining_quantity -= executed_quantity


                    action = "Bought" if order_type == "buy" else "Sold"
                    self.logger_object['info'].log(f"✅ Order {trade_id} {action} {executed_quantity} shares at ${price}. Remaining: {remaining_quantity}")
                    # Accumulate for weighted average calculation
                    total_executed_quantity += executed_quantity
                    total_executed_value += executed_quantity * price    
                    self.logger_object['success'].log(f"🚫 total_executed_value {total_executed_value}")
                    dict_for_sltp = {"user_id": user_id, "portfolio_id": portfolio_id, "stockId": stockId, "stoploss":stoploss, "takeprofit":takeprofit, 'symbol': symbol, "createdBy": createdBy, "asset_type": asset_type}
                    self.db_manager.update_mongo_record(trade_id=trade_id, remaining_quantity=remaining_quantity, executed_quantity=executed_quantity, price=price, total_executed_qty=total_executed_quantity, total_executed_value=total_executed_value, event=event, user_requested_quantity=user_requested_quantity, user_id=user_id, portfolio_id=portfolio_id, dict_for_sltp=dict_for_sltp)
                    self.logger_object['success'].log(f"✅ Successfully update mongoDb Record for trade_id: {trade_id}, remaining_quantity: {remaining_quantity}, executed_quantity: {executed_quantity}")

                    try:               
                        columns = ["entry_price", "quantity", "symbol", "portfolio_id", "average_price"]
                        values= [[price, executed_quantity, symbol, portfolio_id, price]]
                        conflict_columns=["symbol", "portfolio_id"]
                        self.db_manager.upsert_average_transaction(side=event.get("side"), columns=columns, values=values, conflict_columns=conflict_columns, collection_name=avg_transaction_table)
                    except Exception as e:
                        logger_object['info'].log(f"update_mongo_record-upsert_average_transaction: {e}")
                    
                                        # 🔁 Async balance update
                    handler = HandlePortfolioBalanceAndFund(
                        portfolio_id=portfolio_id,
                        asset_name=asset_type,
                        price=price,
                        quantity=executed_quantity,
                        side=order_type,
                        symbol=symbol,
                        trade_id=trade_id,
                        user_id=user_id,
                        close_price=price
                    )

                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(handler.HandlePortfolioHub())
                        else:
                            loop.run_until_complete(handler.HandlePortfolioHub())
                    except RuntimeError:
                        asyncio.run(handler.HandlePortfolioHub())
                    except Exception as e:
                        self.logger_object['error'].log(f"❌ Error running HandlePortfolioHub: {e}")

                except Exception as e:
                    self.logger_object['error'].log(f"🚨 Inner loop error for trade_id {trade_id}: {e}")
                    continue

        except Exception as e:
                self.logger_object['error'].log(f"🚨 Outer error processing order {trade_id}: {e}")

    def process_market_buy(self, event):
        """
        Process a Market Buy Order
        """
        self.process_market_order(event, "buy")

    def process_market_sell(self, event):
        """
        Process a Market Sell Order
        """
        self.process_market_order(event, "sell")