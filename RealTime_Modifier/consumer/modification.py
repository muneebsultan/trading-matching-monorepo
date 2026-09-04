import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Load environment variables
from dotenv import load_dotenv
load_dotenv()


from Core.general import DatabaseManager  
from Core.__init__ import logger_object
logger_object['info'].log("🟢 Logger test at script startup")

from Orchestration.Orchestration import CancelTradeOrchestrator, CancelPendingTrade
cancel_trade = CancelTradeOrchestrator()

class KafkaOrderUpdateConsumer:
    def __init__(self):
        """
        Initialize Kafka Consumer and Database Manager.
        """
        logger_object['info'].log("🔄 Initializing KafkaOrderUpdateConsumer...")
        self.modify_topic = os.getenv("KAFKA_MODIFY_TOPIC", "Stocks-Order-Modified")
        self.kafka_servers = os.getenv("KAFKA_BROKERS").split(",")
        self.group_id = os.getenv("KAFKA_GROUP_ID")

        logger_object['info'].log(f"📝 Configuration - Topic: {self.modify_topic}, Group: {self.group_id}")
        logger_object['info'].log(f"📡 Kafka Brokers: {self.kafka_servers}")

        # Database Manager
        self.db_manager = DatabaseManager()
        logger_object['info'].log("✅ Database Manager initialized")

        # Initialize Kafka Consumer
        self.consumer = None
        logger_object['info'].log("✅ KafkaOrderUpdateConsumer initialized")

    def get_kafka_consumer(self):
        """
        Creates a Kafka consumer with auto-reconnect.
        """
        retries = 5
        for attempt in range(retries):
            try:
                logger_object['info'].log(f"🔄 Attempting to connect to Kafka (Attempt {attempt+1}/{retries})")
                logger_object['info'].log(f"📡 Connecting to brokers: {self.kafka_servers}")
                logger_object['info'].log(f"📝 Using topic: {self.modify_topic}, group: {self.group_id}")
                
                consumer = KafkaConsumer(
                    self.modify_topic,
                    bootstrap_servers=self.kafka_servers,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    key_deserializer=lambda k: json.loads(k.decode("utf-8")) if k else None,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id=self.group_id
                )
                logger_object['info'].log("✅ Kafka Consumer initialized successfully.")
                return consumer
            except KafkaError as e:
                logger_object['error'].log(f"❌ Kafka Consumer init failed (Attempt {attempt+1}/{retries}): {e}")
                time.sleep(5)  # Wait before retrying

        logger_object['error'].log("🚨 Kafka Consumer failed after multiple retries.")
        return None

    def process_order_modification(self, event):
        """
        Process an order modification event and update Mongo.
        """
        trade_id = event.get("trade_id")
        entry_price = event.get("updated_price")

        if not trade_id:
            logger_object['warning'].log(f"⚠️ Missing trade_id in modification event: {event}")
            return

        existing_order = self.db_manager.get_order_from_mongo(trade_id)
        if not existing_order:
            logger_object['warning'].log(f"⚠️ Order {trade_id} not found in Mongo. Cannot modify.")
            return

        self.db_manager.update_modify_trade_status(trade_id, 'Modified',entry_price)
        self.db_manager.update_mongo_all_records(trade_id, event,entry_price, status="Modified")

        logger_object['info'].log(f"🔄 Order {trade_id} modified in Mongo with updated price {entry_price}")

    def process_order_deletion(self, event):
        """
        Process an order deletion event and update Mongo.
        """
        try:
            trade_id = event.get("trade_id")
            print("trade_id", trade_id)  # Moved print after trade_id is defined

            if not trade_id:
                logger_object['error'].log(f"⚠️ Missing trade_id in deletion event: {event}")
                return

            order_data = self.db_manager.get_order_from_mongo(trade_id)
            
            if not order_data:
                logger_object['error'].log(f"⚠️ Order {trade_id} not found in Mongo. Cannot delete.")
                return
            
            if order_data.get("status").capitalize() == "Filled":
                logger_object['info'].log(f"🔄 Order {trade_id} is already filled. Cannot delete.")
                return
            
            if order_data.get("status").capitalize() == "Pending":
                try:
                    cancel_pending_trade = CancelPendingTrade(trade_id=trade_id)
                    cancel_pending_trade.delete_trade_from_order_trades()

                    pending_trade = {'username': order_data.get("username", None), 'symbol': order_data.get("symbol"), 'quantity': order_data.get("quantity") ,'order_type': order_data.get("order_type"), 'side': order_data.get("side"), 'entry_price': order_data.get("entry_price"), 'portfolio_id': order_data.get("portfolio_id"), 'user_id': order_data.get("user_id")}            
                    cancel_pending_trade.add_record_in_transaction_table(pending_trade=pending_trade)
                except Exception as e:
                    logger_object['error'].log(f"⚠️ Error in Cancel Pending Trade: {e}")
                    raise  # Re-raise to be caught by outer try-catch
            else:
                try:
                    #for volume 
                    quantity = order_data.get("quantity")
                    execution = order_data.get('executions', [])
                    if execution:
                        remaining_quantity = execution[-1].get('remaining_quantity', 0)
                        volume = quantity - remaining_quantity

                    logger_object['success'].log(f"volume: {volume}.")

                    partial_entry = {'username': order_data.get("username", None), 'symbol': order_data.get("symbol"), 'remaining_quantity': remaining_quantity , 'volume': volume, 'order_type': order_data.get("order_type"), 'side': order_data.get("side"), 'entry_price': order_data.get("entry_price"), 'portfolio_id': order_data.get("portfolio_id"), 'user_id': order_data.get("user_id"), 'trade_id': order_data.get("trade_id")}
                    cancel_trade.delete_old_transaction(trade_id=order_data.get("trade_id"))
                    cancel_trade.cancel_trade_update_alltransactions(partial_entry=partial_entry)
                    cancel_trade.update_order_trade_table(partial_entry=partial_entry, trade_id=order_data.get("trade_id"))
                except Exception as e:
                    logger_object['error'].log(f"process_order_deletion: {e}")
                    raise  # Re-raise to be caught by outer try-catch
        except Exception as e:
            logger_object['error'].log(f"⚠️ Error in Cancel Trade: {e}")
            # You might want to add retry logic here or notify a monitoring system

    def kafka_listener(self):
        """
        Kafka Consumer Listener - Processes messages with auto-recovery.
        """
        while True:
            try:
                if not self.consumer:
                    logger_object['info'].log("🔄 Attempting to initialize Kafka Consumer...")
                    self.consumer = self.get_kafka_consumer()
                    if not self.consumer:
                        logger_object['error'].log("❌ Kafka Consumer unavailable. Retrying in 5 seconds...")
                        time.sleep(5)
                        continue
                    logger_object['info'].log(f"✅ Connected to Kafka topic: {self.modify_topic}")

                logger_object['info'].log("👂 Listening for messages...")
                for message in self.consumer:
                    try:
                        event = message.value
                        action = event.get("action")
                        trade_id = event.get("trade_id")

                        logger_object['info'].log(f"📥 Received message - Action: {action}, Trade ID: {trade_id}")
                        
                        if not trade_id:
                            logger_object['warning'].log("⚠️ Event missing trade_id. Skipping.")
                            continue

                        if action == "Modified":
                            logger_object['info'].log(f"🔄 Processing modification for trade: {trade_id}")
                            self.process_order_modification(event)
                        elif action == "Deleted":
                            logger_object['info'].log(f"🗑️ Processing deletion for trade: {trade_id}")
                            self.process_order_deletion(event)
                        else:
                            logger_object['warning'].log(f"⚠️ Unknown action received: {action}")

                    except Exception as e:
                        logger_object['error'].log(f"🚨 Error processing message: {str(e)}")
                        logger_object['error'].log(f"Message content: {message.value if message else 'No message content'}")

            except KafkaError as e:
                logger_object['error'].log(f"⚠️ Kafka Consumer error: {str(e)}. Restarting consumer...")
                if self.consumer:
                    self.consumer.close()
                    self.consumer = None  # Reset the consumer
                time.sleep(5)  # Wait before retrying
            except Exception as e:
                logger_object['error'].log(f"🚨 Unexpected error in kafka_listener: {str(e)}")
                time.sleep(5)  # Wait before retrying

    def start(self):
        """
        Start Kafka Consumer and keep it running.
        """
        logger_object['info'].log("🚀 Kafka Consumer started. Listening for order updates...")
        try:
            self.kafka_listener()
        except KeyboardInterrupt:
            logger_object['info'].log("\n🛑 Shutting down consumer...")
            if self.consumer:
                self.consumer.close()
            logger_object['info'].log("✅ Kafka Consumer shut down successfully.")

if __name__ == "__main__":

    print("🚀 modification.py started")
    logger_object['info'].log("🚀 modification.py started")
    
    print("Kafka brokers:", os.getenv("KAFKA_BROKERS"))
    logger_object['info'].log(f"KAFKA_BROKERS: {os.getenv('KAFKA_BROKERS')}")

    consumer = KafkaOrderUpdateConsumer()
    consumer.start()
