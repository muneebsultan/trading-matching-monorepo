from datetime import datetime, timezone
import time
import threading
import json
import os
import signal
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from bson import ObjectId 

from Core.general import DatabaseManager  
from consumers.market_orders import StockOrderProcessor
from Core.nats_price_service import NATSPriceService 
from Core.__init__ import logger_object

load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

# === CONFIGURATION ===
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
TOPIC = os.getenv("KAFKA_STOP_ORDERS_TOPIC", "stop_orders")
KAFKA_RETRIES = int(os.getenv("KAFKA_RETRIES", 3))
CHECK_INTERVAL_SECONDS = int(os.getenv("STOP_ORDER_CHECK_INTERVAL", 1))

# === GLOBALS ===
producer = None
producer_lock = threading.Lock()
shutdown_event = threading.Event()

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
                    linger_ms=5,
                    retries=KAFKA_RETRIES,
                    acks='all',
                    max_in_flight_requests_per_connection=5,
                    request_timeout_ms=10000,
                    connections_max_idle_ms=3600000,
                )
                logger_object["info"].log("✅ Kafka Producer initialized successfully")
            except KafkaError as e:
                logger_object["error"].log(f"❌ Kafka Producer initialization failed: {str(e)}")
                producer = None
    return producer

def evaluate_order_trigger(order, market_price, direction):
    """Determines if an order is triggered based on market price."""
    order_type = order["order_type"]
    
    try:
        entry_price_raw = order.get("entry_price")
        if isinstance(entry_price_raw, list) and entry_price_raw:
            entry_price = float(entry_price_raw[0])
        else:
            entry_price = float(entry_price_raw)
    except (TypeError, ValueError, IndexError) as e:
        logger_object["error"].log(f"❌ Invalid entry price for trade_id {order.get('trade_id')}: {e}")
        return False, None 

    trade_id = order.get("trade_id")

    if direction == "long":
        if order_type == "stoploss" and market_price <= entry_price:
            logger_object["info"].log(f"✅ Triggered Stop-Loss for long trade_id {trade_id} at market_price {market_price} <= entry_price {entry_price}")
            return True, "Stop-Loss"
        if order_type == "takeprofit" and market_price >= entry_price:
            logger_object["info"].log(f"✅ Triggered Take-Profit for long trade_id {trade_id} at market_price {market_price} >= entry_price {entry_price}")
            return True, "Take-Profit"
    elif direction == "short":
        if order_type == "stoploss" and market_price >= entry_price:
            logger_object["info"].log(f"✅ Triggered Stop-Loss for short trade_id {trade_id} at market_price {market_price} >= entry_price {entry_price}")
            return True, "Stop-Loss"
        if order_type == "takeprofit" and market_price <= entry_price:
            logger_object["info"].log(f"✅ Triggered Take-Profit for short trade_id {trade_id} at market_price {market_price} <= entry_price {entry_price}")
            return True, "Take-Profit"

    logger_object["info"].log(
        f"⏳ No trigger for trade_id {trade_id}: direction={direction}, order_type={order_type}, market_price={market_price}, entry_price={entry_price}"
    )
    return False, None


class StopOrderScheduler:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.stock_processor = StockOrderProcessor()
        self.kafka_producer = get_kafka_producer()
        self.price_service = NATSPriceService()
        self.triggered_orders_cache = set()  # To avoid duplicate triggers per run
        self.processed_trades = set()

    def send_to_kafka(self, message):
        """Send a message to Kafka with limited retries and exponential backoff."""
        trade_id = message.get("trade_id", "unknown")
        if not self.kafka_producer:
            logger_object["error"].log(f"❌ Kafka Producer unavailable. Attempting reinitialization...")
            self.kafka_producer = get_kafka_producer()
            if not self.kafka_producer:
                logger_object["error"].log(f"❌ Kafka Producer reinitialization failed. Dropping message for trade_id {trade_id}.")
                return

        for attempt in range(KAFKA_RETRIES):
            try:
                future = self.kafka_producer.send(TOPIC, key=trade_id, value=message)
                future.get(timeout=5)
                logger_object['info'].log(f"📤 Sent order {trade_id} to Kafka successfully.")
                return
            except KafkaError as e:
                logger_object["error"].log(f"🚨 Kafka send failed for {trade_id}, attempt {attempt+1}/{KAFKA_RETRIES}: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
        logger_object["error"].log(f"❌ All Kafka retries failed for trade_id {trade_id}. Message dropped.")

    def check_stop_orders_once(self):
        """Single pass check for pending stop-loss/take-profit orders."""
        try:
            pending_orders = self.db_manager.get_pending_stop_orders()
            logger_object['info'].log(f"📦 Found {len(pending_orders)} pending orders")
            if not pending_orders:
                logger_object["info"].log(" No pending stop/takeprofit orders to check.")
                return

            for order in pending_orders:
                logger_object['info'].log(f"📦 Found pending order now execution with the following detail: {order}")
                trade_id = order.get('trade_id')
                symbol = order.get("symbol", "").upper()
                portfolio_id = order.get("portfolio_id")
                order_type = order.get("order_type")
                user_trade_time = order.get("entry_date")
                user_id = order.get("user_id")

                if not symbol or not trade_id:
                    logger_object["warning"].log(f"⚠️ Skipping invalid or filled order: {order}")
                    continue
                    
                    # ✅ Prevent overlapping execution for same portfolio & symbol
                # if self.is_another_order_in_progress(portfolio_id, symbol, trade_id, order_type):
                if self.is_another_order_in_progress(portfolio_id, symbol, trade_id):
                    logger_object["info"].log(f"⏸️ Skipping trade_id {trade_id} — another Partial_Filled or Pending order exists for {symbol} in portfolio {portfolio_id}")
                    continue

                user_holding_symbol_wise = self.user_holdings(portfolio_id=portfolio_id, symbol=symbol)
                if user_holding_symbol_wise:
                    direction = user_holding_symbol_wise[0].get("direction")
                    side = self.determine_exit_side(holding_side=direction, exit_type=order_type)

                    # Get latest price once per symbol
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
                        logger_object['info'].log(f"New stock price {stock_data}")

                    _, _, ask_price, _, bid_price, _ = stock_data
                    market_price = ask_price if side == "buy" else bid_price

                    # Evaluate trigger
                    triggered, trigger_type = evaluate_order_trigger(order, market_price, direction)
                    if triggered:
                        # Prevent duplicate triggers in the same run
                        unique_trigger_key = f"{trade_id}_{trigger_type}"
                        if unique_trigger_key in self.triggered_orders_cache:
                            continue
                        self.triggered_orders_cache.add(unique_trigger_key)
                        
                        logger_object['info'].log(f"datetime: {user_trade_time}")

                        message = {
                            "trade_id": trade_id,
                            "symbol": symbol.upper(),
                            "side": side,
                            "market_price": market_price,
                            "entry_price": order.get("entry_price"),
                            "direction": direction,
                            "order_type": order.get("order_type"),
                            "trigger_type": trigger_type,
                            "quantity": order.get('quantity'),
                            "user_trade_time": user_trade_time.isoformat(),
                            "portfolio_id": portfolio_id,
                            "user_id": user_id
                        }

                        self.update_trade_status(trade_id=trade_id, status="Partial_Filled")
                        self.add_record_in_transaction(price=order.get("entry_price"), order_type=order.get("order_type"), user_id=user_id, portfolio_id=portfolio_id, symbol=symbol.upper(), trade_id=trade_id)
                        self.send_to_kafka(message)
                        logger_object['info'].log(f"✅ {trigger_type}    {trade_id} at {market_price}")
                else:
                    logger_object['info'].log(f"No Holdings found for portfolio_id: {portfolio_id} and symbol {symbol}")
                    self.update_trade_status(trade_id=trade_id, status='Inactive', collection_name=trade_table)
                    self.update_trade_status(trade_id=trade_id, status='Inactive', collection_name=transaction_table, transaction=True)

        except Exception as e:
            logger_object['error'].log(f"🚨 Error in Stop Order Scheduler: {str(e)}")
    
    def user_holdings(self, portfolio_id, symbol):
        try:
            if portfolio_id is not None and symbol is not None:
                holding_symbol_vise_data = self.db_manager.select(
                        collection_name=avg_transaction_table,
                        filter_criteria={'portfolio_id': str(portfolio_id), 'symbol': symbol}
                    )
                
                if holding_symbol_vise_data:    
                    return holding_symbol_vise_data
                else:
                    logger_object['error'].log(f"holding is empty for the portfolio_id: {portfolio_id} and symbol: {symbol}")    
                    return None

        except Exception as e:
            logger_object['error'].log(f"get_quantity_from_holdings: error in getting quantity {e}")
    
    def is_another_order_in_progress(self, portfolio_id, symbol, current_trade_id):
        try:
            # Check for any pending or partially filled orders for the given portfolio and symbol
            in_progress_orders = self.db_manager.select(
                collection_name=trade_table,
                filter_criteria={
                    "portfolio_id": str(portfolio_id),
                    "symbol": symbol.upper(),
                    "status": {"$in": ["Pending", "Partial_Filled"]}  # Check for pending or partially filled orders
                },
                sort_criteria=[("created_at", 1)]  # Sort by created_at to check the first one
            )

            stop_loss_found = False  # Flag to track if the first stop-loss order has been found
            take_profit_found = False  # Flag to track if the first take-profit order has been found

            for order in in_progress_orders:
                if order.get("trade_id") != current_trade_id:  # Avoid checking the current trade
                    order_type = order.get("order_type")

                    if order_type == "stop-loss":
                        if not stop_loss_found:  # If the first stop-loss order is found, process it
                            stop_loss_found = True
                        else:
                            # If it's another stop-loss, skip it since we've already processed the first one
                            continue

                    elif order_type == "take-profit":
                        if not take_profit_found:  # If it's the first take-profit order, process it
                            take_profit_found = True
                        else:
                            # If it's another take-profit, skip it since we've already processed the first one
                            continue

            # If we found a take-profit order, return True
            if take_profit_found:
                return True

            # No conflicting orders found, return False
            return False

        except Exception as e:
            logger_object["error"].log(f"Error checking active orders: {e}")
            return False

    def determine_exit_side(self, holding_side: str, exit_type: str) -> str:
        """
        Determine whether to use 'buy' or 'sell' to exit a position, 
        based on holding side and exit type (stop loss or take profit).

        Parameters:
        - holding_side (str): 'long' or 'short'
        - exit_type (str): 'stop_loss' or 'take_profit'

        Returns:
        - str: 'buy' or 'sell'

        Raises:
        - ValueError: if inputs are invalid
        """ 
        if holding_side not in ["long", "short"]:
            raise ValueError("holding_side must be 'long' or 'short'")
        
        if exit_type not in ["stoploss", "takeprofit"]:
            raise ValueError("exit_type must be 'stop_loss' or 'take_profit'")

        # Same exit side logic for both stop loss and take profit
        return "sell" if holding_side == "long" else "buy"
        
    def add_record_in_transaction(self, price, order_type, user_id, portfolio_id, symbol, trade_id):
            try:

                order_place_document_transaction = {
                    'transaction_id': ObjectId(), 
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'trade_parameter': 'Partial_Filled',
                    'entry_price': price,
                    'price': price,
                    'portfolio_id': portfolio_id,
                    'user_id':  user_id,
                    'order_type': order_type,
                    'entry_date': datetime.now(timezone.utc)
                }

                self.db_manager.insert(document=order_place_document_transaction, collection=transaction_table, mongodb_client="Traderverse-Authentication")
                logger_object["success"].log(f"Success: successfully add document in transaction table with trade_id: {trade_id}")
            
            except Exception as e:
                logger_object["error"].log(f"Error: add_record_in_transaction: Face error {e}")

    
    def update_trade_status(self, trade_id, status='Filled', collection_name=trade_table, transaction=False):
        try:
            if transaction:
                # ✅ Single atomic Mongo updates (recommend MongoDB transaction if available)
                self.db_manager.update_and_missing_key(
                    collection=collection_name,
                    filer={"trade_id": str(trade_id)},
                    operation={'trade_parameter': status}
                )
            else:
                # ✅ Single atomic Mongo updates (recommend MongoDB transaction if available)
                self.db_manager.update_and_missing_key(
                    collection=collection_name,
                    filer={"trade_id": str(trade_id)},
                    operation={'status': status}
                )
        except Exception as e:
            logger_object['error'].log(f"update_trade_status: error in getting quantity {e}")


    def run(self):
        logger_object["info"].log("⏳ Starting StopOrderScheduler background thread...")
        while not shutdown_event.is_set():
            self.triggered_orders_cache.clear()
            self.check_stop_orders_once()
            time.sleep(CHECK_INTERVAL_SECONDS)

    def start(self):
        """Start scheduler thread and wait for shutdown signal."""
        scheduler_thread = threading.Thread(target=self.run, daemon=True)
        scheduler_thread.start()

        def signal_handler(sig, frame):
            logger_object["info"].log("⚠️ Shutdown signal received, terminating StopOrderScheduler...")
            shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Wait for shutdown
        while not shutdown_event.is_set():
            time.sleep(1)

        # Close Kafka producer cleanly
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
        logger_object["info"].log("✅ StopOrderScheduler shutdown complete.")

    def get_quantity_from_holdings(self, portfolio_id, symbol):
        try:
            if portfolio_id is not None and symbol is not None:
                holding_symbol_vise_data = self.db_manager.select(
                        collection_name=avg_transaction_table,
                        filter_criteria={'portfolios_id': str(portfolio_id), 'symbol': symbol}
                    )
                
                quantity = holding_symbol_vise_data.get("quantity",0)
                return quantity
        
        except Exception as e:
            logger_object['error'].log(f"get_quantity_from_holdings: error in getting quantity {e}")

if __name__ == "__main__":
    scheduler = StopOrderScheduler()
    scheduler.start()
