import time
import threading
import json
import os
import uuid

import warnings
warnings.filterwarnings("ignore")

from kafka import KafkaProducer
from kafka.errors import KafkaError

from Core.general import DatabaseManager  
from consumers.market_orders import StockOrderProcessor

from Core.__init__ import logger_object

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")
TOPIC = "stop_orders"

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
                pending_orders = self.db_manager.get_pending_liquid_orders()
            
                for order in pending_orders:
                    logger_object['info'].log(f"🔍 Checking Limit Order: {order}")
                    trade_id = order["trade_id"]
                    symbol = order["symbol"].upper()
                    side = order["side"]
                    quantity = order["quantity"]
                    liquidation_price =order['liquidation_price']

                    # Ensure it's a single value, not a list
                    if isinstance(order["entry_price"], list):
                        order["entry_price"] = order["entry_price"][0]  # Get the first element if it's a list

                    if isinstance(order["remaining_quantity"], list):
                        order["remaining_quantity"] = order["remaining_quantity"][0]

                    entry_price = float(order["entry_price"])
                    # remaining_quantity = float(order["remaining_quantity"])


                    stock_data = self.db_manager.get_latest_stock_price(symbol)
                    if not stock_data:
                        logger_object["warning"].log(f"⚠️ No market data available for {symbol}")
                        continue

                    _, _, ask_price, _, bid_price, _, _ = stock_data
                    market_price = ask_price if side == "buy" else bid_price
                    check_price = (entry_price - market_price) * quantity
                    if ((entry_price - market_price) * quantity >= liquidation_price):
                        message = {
                            "trade_id":  str(uuid.uuid4()),
                            "user_id": order["user_id"],
                            "trade_id_old": trade_id,
                            "portfolio_id": order["portfolio_id"],
                            "direction": order["direction"],
                            "symbol": symbol,
                            "side": "buy",
                            "market_price": market_price,
                            "entry_price": entry_price,
                            "quantity": quantity,
                            "trigger_type": "liquidation",
                            "liquidation_price": liquidation_price,
                        }
                        self.db_manager.update_trade_status(trade_id, "Partial-Trades")
                        self.send_to_kafka(message)  # Send order to Kafka
                        logger_object['info'].log(f"✅ stop order matched: {symbol} at {market_price}")

            except Exception as e:
                logger_object['error'].log(f"🚨 Error in Limit Order Scheduler: {str(e)}")

            time.sleep(1)  # Run every 10 seconds

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
