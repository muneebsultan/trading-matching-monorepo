import sys
print(sys.path)

import time
import threading
import json
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from Core.general import DatabaseManager  
from consumers.market_orders import StockOrderProcessor
from Core.__init__ import logger_object
from Core.nats_price_service import NATSPriceService 

import warnings

warnings.filterwarnings("ignore")  # Suppress all warnings

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")
TOPIC = "limit_orders"

# Global producer and lock for thread safety
producer = None
producer_lock = threading.Lock()

def get_kafka_producer():
    """Returns a singleton Kafka producer instance in a thread-safe manner."""
    global producer
    with producer_lock:
        if producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: json.dumps(k).encode("utf-8") if k else None,
                    linger_ms=5,  # Small delay to batch messages
                    retries=5,
                    max_in_flight_requests_per_connection=5,
                    request_timeout_ms=10000,  # Reduced timeout to avoid long hangs
                    connections_max_idle_ms=3600000,
                )
                logger_object["info"].log("✅ Kafka Producer initialized successfully")
            except KafkaError as e:
                logger_object["error"].log(f"❌ Kafka Producer initialization failed: {str(e)}")
                producer = None
    return producer

class LimitOrderScheduler:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.stock_processor = StockOrderProcessor()
        self.kafka_producer = get_kafka_producer()  # Initialize Kafka Producer
        self.processed_trades = set()
        self.price_service = NATSPriceService()

    def send_to_kafka(self, message):
        """ Sends a message to Kafka with error handling & retries """
        trade_id = message.get("trade_id")

        if not self.kafka_producer:
            logger_object["error"].log(f"❌ Kafka Producer unavailable. Retrying...")
            self.kafka_producer = get_kafka_producer()  # Reinitialize producer

        try:
            future = self.kafka_producer.send(TOPIC, key=str(trade_id), value=message)
            future.get(timeout=5)  
            logger_object['info'].log(f"📤 Sent order {trade_id} to Kafka successfully.")
        except KafkaError as e:
            logger_object["error"].log(f"🚨 Kafka send failed for {trade_id}: {e}")
            time.sleep(2)  # Retry delay
            self.send_to_kafka(message)  # Retry sending

    def check_old_limit_orders(self):
        """
        Periodically checks old pending limit orders and executes them if conditions meet.
        Runs continuously in an infinite loop.
        """
        while True:
            try:
                logger_object['info'].log("🔁 Scheduler tick: checking for pending orders")
                pending_orders = self.db_manager.get_pending_limit_orders()
                logger_object['info'].log(f"📦 Found {len(pending_orders)} pending orders")
            
                for order in pending_orders:
                    logger_object['info'].log(f"🔍 Checking Limit Order: {order}")
                    trade_id = order["trade_id"]
                    symbol = order["symbol"]
                    side = order["side"]

                    user_trade_time = order.get("entry_date")
                    if isinstance(user_trade_time, datetime):
                        user_trade_time = user_trade_time.isoformat()

                    if not user_trade_time:
                        logger_object['warning'].log(f"⚠️ No entry_date for trade_id: {trade_id}")
                        continue

                    # Ensure it's a single value, not a list
                    if isinstance(order["entry_price"], list):
                        order["entry_price"] = order["entry_price"][0]  # Get the first element if it's a list

                    if isinstance(order["remaining_quantity"], list):
                        order["remaining_quantity"] = order["remaining_quantity"][0]

                    limit_price = float(order["entry_price"])
                    remaining_quantity = float(order["remaining_quantity"])

                    # new_datetime = datetime.now(timezone.utc)
                    # user_trade_time = new_datetime.isoformat()
                    if trade_id not in self.processed_trades:
                        # stock_data = self.db_manager.get_latest_stock_price(symbol, user_trade_time=user_trade_time)
                        stock_data = self.price_service.get_latest_stock_price(symbol.upper(), user_trade_time=user_trade_time)
                        if stock_data:
                            self.processed_trades.add(trade_id)
                    else:
                        # stock_data = self.db_manager.get_latest_stock_price(symbol)
                        stock_data = self.price_service.get_latest_stock_price(symbol.upper())
                        
                    if not stock_data:
                        logger_object['info'].log(f"❌ Order {trade_id} No market price data for {symbol}")
                        continue  
                    
                    if stock_data:
                        logger_object['success'].log(f"New stock price {stock_data}")

                    _, _, ask_price, _, bid_price, _= stock_data
                    market_price = ask_price if side == "buy" else bid_price

                    if (side == "buy" and market_price <= limit_price) or (side == "sell" and market_price >= limit_price):
                        message = {
                            "trade_id": trade_id,
                            "symbol": symbol,
                            "side": side,
                            "market_price": market_price,
                            "entry_price": limit_price,
                            "remaining_quantity": remaining_quantity,
                        }
                        self.db_manager.update_trade_status(trade_id, "Partial-Trades")
                        self.send_to_kafka(message)  # Send order to Kafka
                        logger_object['info'].log(f"✅ Limit order matched: {symbol} at {market_price}")

            except Exception as e:
                import traceback
                logger_object['error'].log(f"🚨 Error in Limit Order Scheduler: {str(e)}")
                logger_object['error'].log(traceback.format_exc())

            time.sleep(10)  # Make sure the loop isn't too tight

    def start(self):
        """ Runs the scheduler in a **single background thread** that stays alive forever. """
        logger_object["info"].log("⏳ Starting Background Scheduler for Limit Orders...")

        scheduler_thread = threading.Thread(target=self.check_old_limit_orders, daemon=True)
        scheduler_thread.start()

        # Keep the main thread alive
        while True:
            time.sleep(60)

if __name__ == "__main__":
    scheduler = LimitOrderScheduler()
    scheduler.start()
