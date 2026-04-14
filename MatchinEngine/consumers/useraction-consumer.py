import json
import threading
import os
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from consumers.limit_orders import LimitOrderProcessor
from consumers.market_orders import StockOrderProcessor
from consumers.stop_loss_orders import StopLossOrderProcessor
from Core.general import DatabaseManager  
from Core.__init__ import logger_object

# Load environment variables
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

class KafkaOrderConsumer:
    def __init__(self):
        """
        Initialize Kafka Consumer and Order Processors.
        """
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "Stocks-Order-Placed")
        self.kafka_servers = os.getenv("KAFKA_BROKERS").split(",")
        self.group_id = os.getenv("KAFKA_GROUP_ID")

        self.db_manager = DatabaseManager()
        
        # Initialize Order Processors
        self.market_processor = StockOrderProcessor()
        self.limit_processor = LimitOrderProcessor()
        self.stop_loss_processor = StopLossOrderProcessor()

        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=128)

        # Initialize Kafka Consumer (Lazy Initialization)
        self.consumer = None
        self.consumer_lock = threading.Lock()  # Prevent race conditions

    def get_kafka_consumer(self):
        """
        Create and return a Kafka consumer with auto-reconnection.
        """
        with self.consumer_lock:
            if self.consumer is not None:
                return self.consumer

            retries = 5
            for attempt in range(retries):
                try:
                    self.consumer = KafkaConsumer(
                        self.kafka_topic,
                        bootstrap_servers=self.kafka_servers,
                        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        key_deserializer=lambda k: k.decode("utf-8") if k else None,
                        auto_offset_reset="earliest",
                        enable_auto_commit=True,
                        group_id=self.group_id,
                        consumer_timeout_ms=10000,  # Ensures no infinite blocking
                    )
                    logger_object["info"].log("✅ Kafka Consumer initialized successfully.")
                    return self.consumer
                except KafkaError as e:
                    logger_object["error"].log(f"❌ Failed to initialize Kafka Consumer (Attempt {attempt+1}/{retries}): {str(e)}")
                    time.sleep(2)

            logger_object["error"].log("🚨 Kafka Consumer initialization failed after multiple attempts.")
            return None

    def process_order(self, event):
        """
        Determines the order type and processes it accordingly.
        """
        order_type = event.get("order_type")
        order_action = event.get("side")
        trade_id = event.get("trade_id")
        event["entry_date"] = datetime.now(timezone.utc)
        logger_object["info"].log(f"📌 Processing {order_type.upper()} Order: {order_action} {event.get('quantity')}")
        
        if not event.get("user_id") or not trade_id:
            logger_object["error"].log("⚠️ Event missing user_id or trade_id. Skipping.")
            return

        trade_order = self.db_manager.select(
                        collection_name=trade_table,
                        filter_criteria={'trade_id': str(trade_id)}
                    )
        if not trade_order:
            self.db_manager.save_first_record(event)
            self.db_manager.update_mongo_all_records(trade_id, event, status="Pending")

        if order_type == "market":
            self.market_processor.process_market_buy(event) if order_action == "buy" else self.market_processor.process_market_sell(event)

        logger_object["success"].log(f"📌 Successfully processed {order_type.upper()} Order: {order_action} {event.get('quantity')}")

    def kafka_listener(self):
        """
        Listens to Kafka messages and processes them asynchronously.
        """
        while True:
            try:
                consumer = self.get_kafka_consumer()
                if consumer is None:
                    logger_object["error"].log("❌ Kafka Consumer is unavailable. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue

                for message in consumer:
                    try:
                        event = message.value
                        logger_object["info"].log(f"📩 Received event: {event}")
                        self.executor.submit(self.process_order, event)
                    except Exception as e:
                        logger_object["error"].log(f"🚨 Unexpected error processing message: {str(e)}")

            except KafkaError as e:
                logger_object["error"].log(f"⚠️ Kafka Consumer encountered an error: {str(e)}. Restarting consumer...")
                with self.consumer_lock:
                    if self.consumer:
                        self.consumer.close()
                        self.consumer = None  # Reset the consumer instance
                time.sleep(5)  # Short delay before retrying

    def start(self):
        """
        Starts the Kafka Consumer in a separate thread and handles graceful shutdown.
        """
        kafka_thread = threading.Thread(target=self.kafka_listener, daemon=True)
        kafka_thread.start()

        try:
            while True:
                time.sleep(5)  # Keeps the main thread alive
        except KeyboardInterrupt:
            logger_object["info"].log("\n🛑 Shutting down consumer...")
            self.executor.shutdown(wait=True)
            if self.consumer:
                self.consumer.close()
            logger_object["info"].log("✅ Kafka Consumer shut down successfully.")


if __name__ == "__main__":
    consumer = KafkaOrderConsumer()
    consumer.start()
