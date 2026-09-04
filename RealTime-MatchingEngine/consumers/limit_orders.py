import json
from datetime import datetime, timezone
from kafka import KafkaConsumer
from Core.general import DatabaseManager  
from Core.__init__ import logger_object
import os
import time
import asyncio
from Core.nats_price_service import NATSPriceService 
from Core.handle_portfolio_balance_and_fund import HandlePortfolioBalanceAndFund
from Core.recalculate_metrics_for_portfolio import AlertForRecalculatePortfolio

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")
TOPIC = "limit_orders"
GROUP_ID = "limit_order_group" 

from dotenv import load_dotenv
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

alert_for_recalculating = AlertForRecalculatePortfolio()

class LimitOrderProcessor:
    def __init__(self):
        """
        Initialize MongoDB and Kafka Consumer.
        """
        self.db_manager = DatabaseManager()
        self.logger_object = logger_object

        # Initialize Kafka Consumer
        self.consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest"  # Ensure processing from the latest uncommitted message
        )

    def process_limit_order(self, event, side):
        """
        Process Limit Buy/Sell Orders
        """
        price_service = NATSPriceService()
        trade_id = event.get("trade_id")
        symbol = event.get("symbol", "").upper()
        remaining_quantity = event.get("remaining_quantity")
        entry_price = float(event.get("entry_price"))
        event["entry_date"] = datetime.now(timezone.utc)
        stoploss = event.get("stop_loss")
        takeprofit = event.get("take_profit")
        stockId = event.get("stockId")

        self.logger_object['success'].log(f"🔄trade_id: {trade_id}, symbol: {symbol}, remaining_quantity: {remaining_quantity} , entry_price : {entry_price}")

        if side == "buy":
            self.logger_object['info'].log(f"📥 Processing Limit Buy Order {trade_id} for {symbol} - Quantity: {remaining_quantity} at ${entry_price}")
        else:
            self.logger_object['info'].log(f"📤 Processing Limit Sell Order {trade_id} for {symbol} - Quantity: {remaining_quantity} at ${entry_price}")


        total_executed_quantity = 0
        total_executed_value = 0  
        entry_time_str = event["entry_date"].isoformat()
        first_run = True

        while True:
            try:
                order_data = self.db_manager.get_order_from_mongo(trade_id)

                order_status = order_data.get("modify_status")
                user_requested_quantity  = order_data.get("quantity")
                user_id = order_data.get("user_id")
                portfolio_id = order_data.get("portfolio_id")
                asset_type = order_data.get("asset_type")
                createdBy = order_data.get("createdBy")

                remaining_quantity = float(order_data.get("remaining_quantity"))
                if remaining_quantity <= 0:
                    self.logger_object['info'].log(f"✅ Order {trade_id} fully executed.")
                    break

                if first_run:
                    user_trade_time = entry_time_str
                    # stock_data = self.db_manager.get_latest_stock_price(symbol, user_trade_time=user_trade_time)
                    stock_data = price_service.get_latest_stock_price(symbol.upper(), user_trade_time=entry_time_str)

                    if stock_data:
                        first_run = False
                        self.db_manager.insert_all_transaction_records(trade_id, event,status="Partial Filled")

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
                            self.logger_object['error'].log(f"❌ Error running Recalculating Limit Partial Filled: {e}")
                        
                else:
                    # stock_data = self.db_manager.get_latest_stock_price(symbol, user_trade_time=None)
                    stock_data = price_service.get_latest_stock_price(symbol.upper(), user_trade_time=None)


                if not stock_data:
                    self.logger_object['info'].log(f"❌ No market price data for {symbol} at {user_trade_time} : {stock_data}")
                    continue
                
                if stock_data:
                    self.logger_object['success'].log(f"New stock price {stock_data}")

                _, _, ask_price, ask_volume, bid_price, bid_volume = stock_data
                price, volume = (ask_price, ask_volume) if side == "buy" else (bid_price, bid_volume)

                if price is None or volume is None or volume == 0:
                    self.logger_object['info'].log(f"🚫 Invalid price/volume for {symbol}. Skipping...")
                    continue

                if (side == "buy" and price <= entry_price) or (side == "sell" and price >= entry_price):
                    executed_quantity = min(remaining_quantity, volume)
                    remaining_quantity -= executed_quantity

                    action = "Bought" if side == "buy" else "Sold"
                    self.logger_object['info'].log(f"✅ Order {trade_id} {action} {executed_quantity} shares at ${price}. Remaining: {remaining_quantity}")

                    total_executed_quantity += executed_quantity
                    total_executed_value += executed_quantity * price

                    self.logger_object['success'].log(f"remaining_quantity: {remaining_quantity}, executed_quantity:{executed_quantity}, price:{price}, total_executed_quantity: {total_executed_quantity}, total_executed_value:{total_executed_value}")
                    dict_for_sltp = {"user_id": user_id, "portfolio_id": portfolio_id, "stockId": stockId, "stoploss":stoploss, "takeprofit":takeprofit, "createdBy":createdBy, "asset_type":asset_type}
                    self.db_manager.update_mongo_record(trade_id=trade_id, remaining_quantity=remaining_quantity, executed_quantity=executed_quantity, price=price, total_executed_qty=total_executed_quantity, total_executed_value=total_executed_value, event=event, user_requested_quantity=user_requested_quantity, user_id=user_id, portfolio_id=portfolio_id, dict_for_sltp=dict_for_sltp)
                    logger_object['success'].log(f"yahan tk agyaa hai clear hai")

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
                        side=side,
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

                else:
                    self.logger_object['info'].log(f"⏳ Order {trade_id} Waiting: Current price ${price} does not meet limit ${entry_price}")

            except Exception as e:
                self.logger_object['error'].log(f"🚨 Error processing limit order {trade_id}: {e}")
                break

    def process_limit_buy(self, event):
        """ Process a Limit Buy Order """
        self.process_limit_order(event, "buy")

    def process_limit_sell(self, event):
        """ Process a Limit Sell Order """
        self.process_limit_order(event, "sell")

    def consume_kafka_messages(self):
        """ Continuously listen to Kafka topic and process orders """
        self.logger_object['info'].log(f"🎧 Listening for messages on Kafka topic: {TOPIC}...")

        for message in self.consumer:
            try:
                event = message.value
                side = event.get("side")

                if side == "buy":
                    self.process_limit_buy(event)
                elif side == "sell":
                    self.process_limit_sell(event)
                else:
                    self.logger_object['warning'].log(f"⚠️ Unknown order type received: {side}")

            except Exception as e:
                self.logger_object['error'].log(f"🚨 Error consuming message: {e}")

    def start(self):
        """ Keep Kafka consumer running 24/7 """
        while True:
            try:
                self.consume_kafka_messages()
            except Exception as e:
                self.logger_object['error'].log(f"🚨 Unexpected Error: {e}. Restarting consumer...")
                time.sleep(5)  # Short delay before restarting

if __name__ == "__main__":
    processor = LimitOrderProcessor()
    asyncio.run(processor.start())
