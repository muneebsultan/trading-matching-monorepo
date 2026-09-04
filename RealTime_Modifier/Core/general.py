import os
import json
import redis
import requests
import asyncio
from dateutil import parser
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
from bson import ObjectId 

# Load environment variables
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

from Core.__init__ import logger_object
# Import the new NATS price service
from Core.nats_price_service import NATSPriceService

class DatabaseManager:
    def __init__(self, use_nats=True):
        """
        Initialize MongoDB and Redis connections.
        """

        self.mongo_url = os.getenv("MONGO_URL")
        self.client = MongoClient(
                        self.mongo_url,
                        serverSelectionTimeoutMS=5000,
                        socketTimeoutMS=10000,
                        connectTimeoutMS=10000,
                        maxIdleTimeMS=300000,
                        retryWrites=True
                    )

        self.use_nats = use_nats
        if self.use_nats:
            self.price_service = NATSPriceService()
            logger_object['info'].log("Using NATS for price data")
        else:
            # Legacy Redis-based service
            logger_object['info'].log("Using Redis for price data")


    def check_connection(self):
        """
        Pings MongoDB to ensure the connection is alive.
        Reconnects if the connection is stale or dropped.
        """
        try:
            self.client.admin.command('ping')
        except Exception as e:
            logger_object['error'].log(f"⚠️ MongoDB ping failed, reconnecting: {e}")
            self.client = MongoClient(
                self.mongo_url,
                serverSelectionTimeoutMS=5000,
                socketTimeoutMS=10000,
                connectTimeoutMS=10000,
                maxIdleTimeMS=300000,
                retryWrites=True
            )
            self.mongo_client = self.client  # update fallback reference too

                
    def get_pending_limit_orders(self, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Fetch all pending limit orders from MongoDB.
        Orders must be limit orders and have remaining quantity > 0.
        """
        self.check_connection()
        db = self.client[mongodb_client]
        collection = db[collection_name]
        pending_orders_cursor = collection.find({
            "$and": [
                {"order_type": {"$in": ["limit"]}},  # Fetch limit orders
                {"status": {"$in": ["Pending", "pending"]}},   # Only pending orders
                {"remaining_quantity": {"$gt": 0}}             # With remaining quantity > 0
            ]
        })
        # Await cursor and convert to list
        pending_orders = list(pending_orders_cursor)
        return pending_orders
    
    def get_pending_stop_orders(self, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Fetch all pending limit orders from MongoDB.
        Orders must be limit orders and have remaining quantity > 0.
        """
        db = self.client[mongodb_client]
        collection = db[collection_name]
        pending_orders_cursor = collection.find({
            "$and": [
                {"order_type":  {"$in": ["stoploss", "takeprofit"]}},  
                {"status": {"$in": ["Pending", "pending"]}}, 
                {
                    "$or": [
                        {"remaining_quantity": {"$gt": 0}},
                        {"remaining_quantity": None}
                    ]
                } 
            ]
        })
        # Await cursor and convert to list asynchronously
        pending_orders = list(pending_orders_cursor)
        return pending_orders

    def get_pending_liquid_orders(self, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Fetch all pending limit orders from MongoDB.
        Orders must be limit orders and have remaining quantity > 0.
        """
        db = self.client[mongodb_client]
        collection = db[collection_name]
        pending_orders_cursor = collection.find({
            "$and": [
                {"order_type":  {"$in": ["stoploss", "takeprofit"]}},  
                {"status": "Filled"}, {"direction":{"$in": ["short", "Short"]} }, 
                {"side": "sell"}
            ]
        })

        # Await cursor and convert to list asynchronously
        pending_orders = list(pending_orders_cursor)
        return pending_orders


    def save_first_record(self, event, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        try:
            """
            Save a new order record to MongoDB.
            """
            # Ensure required fields are present
            trade_id = event.get("trade_id")
            if not trade_id:
                logger_object['error'].log("❌ Missing 'trade_id' in event data.")
                return  # Return early if essential field is missing
            
            logger_object['success'].log(f" order_type {event.get('order_type', '')} order type {event.get('price')}")
        
            if event.get("order_type", "").lower() != "market":
                price = event.get("price")
            else:
                price = None
            
            logger_object['success'].log(f" order_type {event.get('order_type', '')} order type {event.get('price')}")
            if event.get("order_type", "").lower() in ['stoploss', 'takeprofit']:
                order_data = {
                    "trade_id": trade_id,
                    "username": event.get("username") if event.get("username") else None,
                    "symbol": event.get("symbol").upper() if event.get("symbol") else None,  # Handle missing symbol
                    "side": event.get("side"),
                    "order_type": event.get("order_type"),
                    "remaining_quantity": event.get("quantity"),
                    "entry_price": price,
                    "price": None,
                    "status": "Pending",
                    "portfolio_id": event.get("portfolio_id"),
                    "user_id": event.get("user_id"),
                    "entry_date": event.get("entry_date"),
                    "liquidation_price": event.get("liquidation_price") if event.get("liquidation_price") else None,
                    "direction": event.get("direction") if event.get("direction") else None,
                    "tif_type": event.get("tif_type").upper() if event.get("tif_type") else None,
                    "createdBy": event.get("createdBy").capitalize() if event.get("createdBy") else None,
                    "asset_type": event.get("asset_type").lower() if event.get("asset_type") else None,
                    "stockId": event.get("stockId").upper() if event.get("stockId") else None,
                    "stop_loss": event.get("stop_loss") if event.get("stop_loss") else None, 
                    "take_profit": event.get("take_profit") if event.get("take_profit") else None,
                    "created_at": datetime.utcnow()
                    }
            else:
                order_data = {
                "trade_id": trade_id,
                "username": event.get("username") if event.get("username") else None,
                "symbol": event.get("symbol").upper() if event.get("symbol") else None,  # Handle missing symbol
                "side": event.get("side"),
                "order_type": event.get("order_type"),
                "remaining_quantity": event.get("quantity"),
                "quantity": event.get("quantity"),
                "entry_price": price,
                "price": None,
                "status": "Pending",
                "portfolio_id": event.get("portfolio_id"),
                "user_id": event.get("user_id"),
                "entry_date": event.get("entry_date"),
                "liquidation_price": event.get("liquidation_price") if event.get("liquidation_price") else None,
                "direction": event.get("direction") if event.get("direction") else None,
                "tif_type": event.get("tif_type").upper() if event.get("tif_type") else None,
                "createdBy": event.get("createdBy").capitalize() if event.get("createdBy") else None,
                "asset_type": event.get("asset_type").lower() if event.get("asset_type") else None,
                "stockId": event.get("stockId").upper() if event.get("stockId") else None,
                "stop_loss": event.get("stop_loss") if event.get("stop_loss") else None, 
                "take_profit": event.get("take_profit") if event.get("take_profit") else None,
                "created_at": datetime.utcnow()
                }

            try:
                # Access the correct database and collection
                db = self.client[mongodb_client]
                collection = db[collection_name]
                collection.insert_one(order_data)  # Insert the data as a new document
                logger_object['info'].log(f"✅ Order {trade_id} saved to MongoDB.")
            except Exception as e:
                # Log the error and raise it if needed
                logger_object['error'].log(f"❌ Failed to save order {trade_id} to MongoDB: {e}")
                raise
        
        except Exception as e:
                # Log the error and raise it if needed
                logger_object['error'].log(f"❌ Failed to save order {trade_id} to MongoDB: {e}")
                raise


    def update_modify_trade_status(self, trade_id, new_status, entry_price=None, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Update the status of a trade in MongoDB, updating entry_price only if provided.

        :param trade_id: Trade ID to update.
        :param new_status: New status to be set.
        :param entry_price: Updated entry price (optional, only updated if provided).
        """
        if not trade_id or not new_status:
            self.logger["error"].error("❌ Invalid trade_id or status provided. Skipping update.")
            return

        try:
            # Prepare update query
            update_fields = {"modify_status": new_status, "last_updated": datetime.utcnow()}
            if entry_price is not None:
                update_fields["entry_price"] = entry_price

            # Execute the update
            db = self.client[mongodb_client]
            collection = db[collection_name]
            result = collection.update_one({"trade_id": trade_id}, {"$set": update_fields})

            if result.modified_count > 0:
                logger_object["info"].log(f"✅ Successfully updated trade {trade_id} to status '{new_status}'.")
            else:
                logger_object["warning"].log(f"⚠️ No changes made. Trade {trade_id} may already have status '{new_status}'.")

        except Exception as e:
            logger_object["error"].log(f"🚨 Error updating trade {trade_id} status: {e}")
    
    def insert(self, document, collection, mongodb_client="Traderverse-Authentication"):
        try:
            db = self.client[mongodb_client]
            collection = db[collection]
            document["created_at"] = datetime.utcnow()
            insert_result = collection.insert_one(document)
        
        except Exception as e:
            logger_object["error"].log(f"insert: 🚨 Error in insertion: {e}")
    

    def update_and_missing_key(self, filer, operation, collection, mongodb_client="Traderverse-Authentication"):
        """
        Ensures all provided keys are updated in the MongoDB document (synchronous).
        - If a key exists and has a different value → Update it.
        - If a key is missing → Add it.
        - If the document does not exist → Create a new document.

        Parameters:
        - filer: Filter criteria (e.g., {"portfolio_id": "12345"})
        - operation: Dictionary of fields to update/add.
        - collection: MongoDB collection name.
        - mongodb_client: MongoDB database name (default: "Traderverse-Authentication")

        Returns:
        - None
        """
        try:
            db = self.client[mongodb_client]
            collection = db[collection]

            # 🔍 Fetch the existing document
            existing_doc = collection.find_one(filer)

            if existing_doc:
                # Only update changed or missing fields
                update_data = {}
                for key, value in operation.items():
                    if key not in existing_doc or existing_doc[key] != value:
                        update_data[key] = value

                if update_data:
                    collection.update_one(filer, {"$set": update_data})
                    print("✅ Document updated with new & changed keys.")
                else:
                    print("✅ No changes detected. Update skipped.")
            else:
                # Document doesn't exist — create a new one
                collection.insert_one({**filer, **operation})
                print("✅ New document created with all fields.")

        except Exception as e:
            print("❌ MongoDB Operation Error:", e)
    
    def set_and_push_mongo_fields(self, filter_query, set_fields, push_fields, collection, mongodb_client="Traderverse-Authentication"):
        """
        Performs both $set and $push operations on a MongoDB document.
        Creates the document if it does not exist.

        Parameters:
        - filter_query: Dictionary to find the document (e.g., {"trade_id": "123"})
        - set_fields: Fields to update or insert (e.g., {"status": "Partial Filled"})
        - push_fields: Fields to push into arrays (e.g., {"executions": {...}})
        - collection: Collection name in MongoDB
        - mongodb_client: Database name

        Returns: None
        """
        try:
            db = self.client[mongodb_client]
            col = db[collection]

            update_data = {}
            if set_fields:
                update_data["$set"] = set_fields
            if push_fields:
                update_data["$push"] = push_fields

            result = col.update_one(filter_query, update_data, upsert=True)

            if result.matched_count:
                print("✅ Document updated with $set and $push.")
            else:
                print("✅ New document created via upsert.")

        except Exception as e:
            print("❌ MongoDB Operation Error:", e)
    
    def update(self, filer, operation, collection, mongodb_client="Traderverse-Authentication"):
        try:
            db = self.client[mongodb_client]
            collection = db[collection]

            collection.update_one(
                                    filer,         
                                    {"$set": operation}       
                                    )

        except Exception as e:
            print("MongodbOperation-update :",e)
    
    def delete_document(self, collection_name, filter_criteria, mongodb_client="Traderverse-Authentication"):
        """
        Delete a document from a MongoDB collection.
        
        :param collection_name: The name of the collection
        :param filter_criteria: The filter criteria to match documents
        :param mongodb_client: The name of the database
        """
        try:
            db = self.client[mongodb_client]
            collection = db[collection_name]
            
            # Delete one or many documents based on the filter criteria
            delete_result = collection.delete_many(filter_criteria)
            
            if delete_result.deleted_count > 0:
                print(f"Deleted {delete_result.deleted_count} document(s) from {collection_name}.")
            else:
                print("No documents matched the criteria.")
        except Exception as e:
            print(f"Error deleting document(s): {e}")



    def save_liquidation_record(self, event, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Save a new order record to MongoDB.
        """
        order_data = {
            "trade_id": event.get("trade_id"),
            "old_trade_id": event.get("trade_id_old"),
            "side": event.get("side"),
            "user_id": event.get("user_id"),
            "remaining_quantity": event.get("quantity"),
            "portfolio_id": event.get("portfolio_id"),
            "order_type": event.get("trigger_type"),
            "entry_date": datetime.utcnow(),
            "symbol": event.get("symbol").upper(),
            "quantity": event.get("quantity"),
            "price": None,
            "entry_price": event.get("entry_price") if event.get("entry_price") else None,
            "status": "Pending",
            "liquidation_price": event.get("liquidation_price") if event.get("liquidation_price") else None,
            "direction": event.get("direction") if event.get("direction") else None,
            "created_at": datetime.utcnow()
        }
        try:
            db = self.client[mongodb_client]
            collection = db[collection_name]
            collection.insert_one(order_data)
            logger_object['info'].log(f"✅ Order {event.get('trade_id')} saved to MongoDB.")
        except Exception as e:
            logger_object['error'].log(f"❌ Failed to save order to MongoDB: {e}")

    # def fetch_quote(self, symbol):
    #     """
    #     Fetch the latest quote for a symbol from Redis using UTC-based key format.
    #     """
    #     QUOTE_PREFIX = os.getenv("QUOTE_PREFIX", "quote:")
    #     REDIS_CLIENT = redis.StrictRedis(
    #         host=os.getenv("REDIS_HOST"),
    #         port=os.getenv("REDIS_PORT"),
    #         db=0,
    #         decode_responses=True
    #     )

    #     redis_key = f"{QUOTE_PREFIX}{symbol}"
    #     try:
    #         quote_json = REDIS_CLIENT.get(redis_key)
    #         if not quote_json:
    #             print(f"No quote found for {symbol}")
    #             return None
    #         return json.loads(quote_json)
    #     except Exception as e:
    #         logger_object['error'].log(f"fetch_quote: ⚠️ Error: {e}")
    #         return None


    # def get_latest_stock_price(self, symbol, user_trade_time=None):
    #     """
    #     Fetch the latest stock price for a symbol from Redis using UTC timestamps.
    #     Ensures that it's newer than the last fetched quote and not a duplicate.
    #     """
    #     try:
    #         if not hasattr(self, '_last_timestamp'):
    #             self._last_timestamp = {}

    #         symbol = symbol.upper()
    #         latest_quote = self.fetch_quote(symbol)
    #         logger_object['error'].log(f"latest_quote: {latest_quote}")

    #         if not latest_quote:
    #             logger_object['error'].log(f"get_latest_stock_price: ⚠️ No quote found for symbol: {symbol}")
    #             return None

    #         latest_time_str = latest_quote.get('timestamp_UTC')
    #         if not latest_time_str:
    #             logger_object['error'].log("Missing 'timestamp_UTC' in quote data")
    #             return None

    #         # Parse and validate the timestamp
    #         latest_time = parser.isoparse(latest_time_str)

    #         # 🚫 Reject garbage timestamps
    #         if latest_time.year < 2000:
    #             logger_object['error'].log(f"Quote timestamp too old or invalid: {latest_time} for {symbol}")
    #             return None

    #         # Check against last seen timestamp
    #         last_seen_time_str = self._last_timestamp.get(symbol)
    #         if last_seen_time_str:
    #             last_seen_time = parser.isoparse(last_seen_time_str)
    #             if latest_time <= last_seen_time:
    #                 logger_object['info'].log(
    #                     f"[SKIP CHECK] latest_time={latest_time} | last_seen_time={last_seen_time} for symbol={symbol}"
    #                 )
    #                 return None

    #         # Compare with user trade time
    #         if user_trade_time:
    #             trade_time = parser.isoparse(user_trade_time)
    #             if latest_time < trade_time:
    #                 logger_object['info'].log(f"get_latest_stock_price: Quote for {symbol} is older than trade time {trade_time}")
    #                 # Optionally reset the tracker
    #                 self._last_timestamp.pop(symbol, None)
    #                 return None

    #         # ✅ Valid quote: update last seen
    #         self._last_timestamp[symbol] = latest_time_str

    #         ask_price = latest_quote.get('askPrice')
    #         bid_price = latest_quote.get('bidPrice')

    #         new_ask_price = float(ask_price) / 10000 if ask_price is not None else None
    #         new_bid_price = float(bid_price) / 10000 if bid_price is not None else None

    #         logger_object['success'].log(f"get_latest_stock_price: {latest_quote} for symbol: {symbol}")

    #         return [
    #             latest_time_str,
    #             latest_quote.get('symbol', ''),
    #             new_ask_price,
    #             float(latest_quote.get('askQuantity', 0)),
    #             new_bid_price,
    #             float(latest_quote.get('bidQuantity', 0)),
    #             latest_quote.get('market', '')
    #         ]

    #     except Exception as e:
    #         logger_object['error'].log(f"get_latest_stock_price: ⚠️ Error while querying Redis: {e}")
    #         return None

    def fetch_quote(self, symbol):
        """
        Fetch the latest quote for a symbol (from NATS or Redis).
        """
        if self.use_nats:
            return self.price_service.fetch_quote(symbol)
        else:
            # Original Redis implementation
            QUOTE_PREFIX = os.getenv("QUOTE_PREFIX", "quote:")
            REDIS_CLIENT = redis.StrictRedis(
                host=os.getenv("REDIS_HOST"),
                port=os.getenv("REDIS_PORT"),
                db=0,
                decode_responses=True
            )

            redis_key = f"{QUOTE_PREFIX}{symbol}"
            try:
                quote_json = REDIS_CLIENT.get(redis_key)
                if not quote_json:
                    print(f"No quote found for {symbol}")
                    return None
                return json.loads(quote_json)
            except Exception as e:
                logger_object['error'].log(f"fetch_quote: ⚠️ Error: {e}")
                return None

    def get_latest_stock_price(self, symbol, user_trade_time=None):
        """
        Fetch the latest stock price for a symbol (from NATS or Redis).
        """
        if self.use_nats:
            return self.price_service.get_latest_stock_price(symbol, user_trade_time)
        else:
            # Original Redis implementation
            try:
                if not hasattr(self, '_last_timestamp'):
                    self._last_timestamp = {}

                symbol = symbol.upper()
                latest_quote = self.fetch_quote(symbol)
                logger_object['error'].log(f"latest_quote: {latest_quote}")

                if not latest_quote:
                    logger_object['error'].log(f"get_latest_stock_price: ⚠️ No quote found for symbol: {symbol}")
                    return None

                latest_time_str = latest_quote.get('timestamp_UTC')
                if not latest_time_str:
                    logger_object['error'].log("Missing 'timestamp_UTC' in quote data")
                    return None

                # Parse and validate the timestamp
                latest_time = parser.isoparse(latest_time_str)

                # 🚫 Reject garbage timestamps
                if latest_time.year < 2000:
                    logger_object['error'].log(f"Quote timestamp too old or invalid: {latest_time} for {symbol}")
                    return None

                # Check against last seen timestamp
                last_seen_time_str = self._last_timestamp.get(symbol)
                if last_seen_time_str:
                    last_seen_time = parser.isoparse(last_seen_time_str)
                    if latest_time <= last_seen_time:
                        logger_object['info'].log(
                            f"[SKIP CHECK] latest_time={latest_time} | last_seen_time={last_seen_time} for symbol={symbol}"
                        )
                        return None

                # Compare with user trade time
                if user_trade_time:
                    trade_time = parser.isoparse(user_trade_time)
                    if latest_time < trade_time:
                        logger_object['info'].log(f"get_latest_stock_price: Quote for {symbol} is older than trade time {trade_time}")
                        # Optionally reset the tracker
                        self._last_timestamp.pop(symbol, None)
                        return None

                # ✅ Valid quote: update last seen
                self._last_timestamp[symbol] = latest_time_str

                ask_price = latest_quote.get('askPrice')
                bid_price = latest_quote.get('bidPrice')

                new_ask_price = float(ask_price) / 10000 if ask_price is not None else None
                new_bid_price = float(bid_price) / 10000 if bid_price is not None else None

                logger_object['success'].log(f"get_latest_stock_price: {latest_quote} for symbol: {symbol}")

                return [
                    latest_time_str,
                    latest_quote.get('symbol', ''),
                    new_ask_price,
                    float(latest_quote.get('askQuantity', 0)),
                    new_bid_price,
                    float(latest_quote.get('bidQuantity', 0)),
                    latest_quote.get('market', '')
                ]

            except Exception as e:
                logger_object['error'].log(f"get_latest_stock_price: ⚠️ Error while querying Redis: {e}")
                return None
            
    def select(self, collection_name, filter_criteria, mongodb_client="Traderverse-Authentication"):
        """
        Select documents from a MongoDB collection (synchronous version).
        
        :param collection_name: The name of the collection
        :param filter_criteria: The filter criteria to match documents
        :param mongodb_client: The name of the database
        :return: List of matching documents
        """
        try:
            db = self.client[mongodb_client]
            collection = db[collection_name]
            
            # Perform the query
            cursor = collection.find(filter_criteria).sort("created_at", -1)
            
            # Convert cursor to a list
            documents = list(cursor)
            
            return documents
        
        except Exception as e:
            print(f"Error selecting document(s): {e}")
            return None
    
    def fetch_user_portfolios(self, collection, user_id=None, portfolio_id=None, single_portfolio=False, mongodb_client="Traderverse-Authentication"):
        try:
            db = self.client[mongodb_client]
            collection = db[collection]
            
            if single_portfolio:
                cursor = collection.find({"_id": str(portfolio_id)})
            else:
                cursor = collection.find({"userId": str(user_id)})
            
            # Convert cursor to a list of results
            id_list = list(cursor)
            return id_list

        except Exception as e:
            print("MongodbOperation-fetch_user_portfolios:", e)
            return None

    def get_updated_price_mongo(self, trade_id, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Fetch complete trade details from MongoDB by trade_id.
        """
        try:
            # trade = self.mongo_client.find_one({"trade_id": trade_id}, {"_id": 0})
            # Fetch only the remaining_quantity and status fields from the trade
            db = self.client[mongodb_client]
            collection = db[collection_name]
            trade = collection.find_one(
                {"trade_id": trade_id}, 
                {"_id": 0, "entry_price": 1}
            )

            if trade:
                logger_object['info'].log(f"📊 Trade {trade_id} fetched: {trade}")
                return float(trade.get("entry_price"))
            else:
                logger_object['info'].log(f"⚠️ Trade {trade_id} not found.")
                return {"error": "Trade not found"}

        except Exception as e:
            logger_object['info'].log(f"❌ Error fetching trade {trade_id}: {str(e)}")
            return {"error": str(e)}

    def get_order_from_mongo(self, trade_id, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Fetch complete trade details from MongoDB by trade_id.
        """
        try:
            # trade = self.mongo_client.find_one({"trade_id": trade_id}, {"_id": 0})
            # Fetch only the remaining_quantity and status fields from the trade
            db = self.client[mongodb_client]
            collection = db[collection_name]
            # trade = collection.find_one(
            #     {"trade_id": trade_id}, 
            #     {"_id": 0, "remaining_quantity": 1, "status": 1}
            # )
            trade = collection.find_one({"trade_id": trade_id})
  
            if trade:
                logger_object['info'].log(f"📊 Trade {trade_id} fetched: {trade}")
                return trade
            else:
                logger_object['info'].log(f"⚠️ Trade {trade_id} not found.")
                return {"error": "Trade not found"}

        except Exception as e:
            logger_object['info'].log(f"❌ Error fetching trade {trade_id}: {str(e)}")
            return {"error": str(e)}


    def update_mongo_record(self, trade_id, remaining_quantity, executed_quantity, price, total_executed_qty, total_executed_value,event, collection_name=trade_table, mongodb_client="Traderverse-Authentication", user_id = None, user_requested_quantity = None, portfolio_id = None):
        try:
            """
            Update a single order record in MongoDB.
            """
            # Prepare MongoDB update
            execution_record = {
                "executed_quantity": executed_quantity,
                "remaining_quantity": remaining_quantity,
                "price": price,
                "timestamp": datetime.utcnow()
            }
            update_query = {
                "$push": {"executions": execution_record},
                "$set": {"status": "Filled" if remaining_quantity == 0 else "Partial Filled",
                                    "remaining_quantity": remaining_quantity  # Update the main remaining_quantity
                        }
            }

            # Update order in MongoDB
            db = self.client[mongodb_client]
            collection = db[collection_name]
            collection.update_one({"trade_id": trade_id}, update_query)

            if total_executed_qty > 0:
                weighted_avg_price = total_executed_value / total_executed_qty
                logger_object['info'].log(f"📊 Final Weighted Avg Price for {trade_id}: ${weighted_avg_price:.2f}")
                collection.update_one(
                    {"trade_id": trade_id},
                    {"$set": {"price": weighted_avg_price}}
                )
            if remaining_quantity == 0:
                try:        
                    logger_object['success'].log(f"andar arhaaa hai.")            
                    columns = ["entry_price", "quantity", "symbol", "portfolio_id", "average_price"]
                    values= [[weighted_avg_price, user_requested_quantity, event.get("symbol").upper(), portfolio_id, weighted_avg_price]]
                    conflict_columns=["symbol", "portfolio_id"]
                    self.upsert_average_transaction(side=event.get("side"), columns=columns, values=values, conflict_columns=conflict_columns, collection_name=avg_transaction_table)
                except Exception as e:
                    logger_object['info'].log(f"update_mongo_record-upsert_average_transaction: {e}")

                transaction_event = {'trade_id': event.get("trade_id"), 'side': event.get("side"), 'symbol':event.get("symbol"), 'portfolio_id': portfolio_id, 'quantity': user_requested_quantity}
                # self.update_all_transaction_records(trade_id, event,weighted_avg_price,status="Filled")
                self.update_all_transaction_records(trade_id, transaction_event,weighted_avg_price,status="Filled")
            
            logger_object['info'].log(f"✅ Order {trade_id} updated in MongoDB.")
        
        except Exception as e:
            logger_object['info'].log(f"❌ Error fetching trade {trade_id}: {str(e)}")
            return {"error": str(e)}

    def update_mongo_all_records(self, trade_id, event,price=None,status="Pending", collection_name=transaction_table, mongodb_client="Traderverse-Authentication"):
        """
        Insert a new trade record in MongoDB for historical tracking.
        """
        order_details = {
            "transaction_id": ObjectId(),  # Unique ID for each transaction
            "username": event.get("username") if event.get("username") else None,
            "symbol": event.get("symbol").upper(),
            "quantity": event.get("quantity"),
            "price": price,
            "entry_date": event.get("entry_date"),
            "trade_parameter": status,
            "order_type": event.get("order_type"),
            "side": event.get("side"),
            "trade_id": event.get("trade_id"),
            "portfolio_id": event.get("portfolio_id"),
            "user_id": event.get("user_id"),
            "liquidation_price": event.get("liquidation_price") if event.get("liquidation_price") else None,
            "tif_type": event.get("tif_type").upper() if event.get("tif_type") else None,
            "createdBy": event.get("createdBy").capitalize() if event.get("createdBy") else None,
            "asset_type": event.get("asset_type").lower() if event.get("asset_type") else None,
            "stockId": event.get("stockId").upper() if event.get("stockId") else None,
            "created_at": datetime.utcnow(),
        }
        

        # Insert into MongoDB (Separate collection for all records)
        db = self.client[mongodb_client]
        collection = db[collection_name]
        collection.insert_one(order_details)

        logger_object['info'].log(f"✅ Order {trade_id} inserted into all records collection in MongoDB.")

    def update_all_transaction_records(self, trade_id, event, price=None, status="Pending", collection_name=transaction_table, mongodb_client="Traderverse-Authentication"):
        """
        Insert or update a trade record in MongoDB for historical tracking.
        """
        # Check if the record already exists in the collection
        db = self.client[mongodb_client]
        collection = db[collection_name]
        existing_record = collection.find_one({"trade_id": trade_id})

        if existing_record:
            # If the record exists, update it with the new details
            update_query = {
                "$set": {
                    "side": event.get("side"),
                    "price": price,
                    "trade_parameter": status,
                    "symbol": event.get("symbol").upper(),
                    "quantity": event.get("quantity"),
                    "created_at": datetime.utcnow(),
                    "portfolio_id": event.get("portfolio_id"),
                    "entry_date": datetime.utcnow()  # Update the entry date if necessary
                }
            }
            collection.update_one({"trade_id": trade_id}, update_query)
            logger_object['info'].log(f"🔄 Order {trade_id} updated in {transaction_table} collection in MongoDB.")
        else:
            order_details = {
            "transaction_id": ObjectId(),  # Unique ID for each transaction
            "username": event.get("username") if event.get("username") else None,
            "symbol": event.get("symbol").upper(),
            "quantity": event.get("quantity"),
            "price": price,
            "entry_date": event.get("entry_date"),
            "trade_parameter": status,
            "order_type": event.get("order_type"),
            "side": event.get("side"),
            "trade_id": event.get("trade_id"),
            "portfolio_id": event.get("portfolio_id"),
            "user_id": event.get("user_id"),
            "liquidation_price": event.get("liquidation_price") if event.get("liquidation_price") else None,
            "tif_type": event.get("tif_type").upper() if event.get("tif_type") else None,
            "createdBy": event.get("createdBy").capitalize() if event.get("createdBy") else None,
            "asset_type": event.get("asset_type").lower() if event.get("asset_type") else None,
            "stockId": event.get("stockId").upper() if event.get("stockId") else None,
            "created_at": datetime.utcnow(),
            }
            # Insert into MongoDB (Separate collection for all records)
            collection.insert_one(order_details)
            logger_object['info'].log(f"✅ Order {trade_id} inserted into all records collection in MongoDB.")

    # def update_trade_status(self, trade_id, new_status, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
    #     """
    #     Update the status of a trade in MongoDB.
        
    #     :param trade_id: Trade ID to update
    #     :param new_status: New status to be set
    #     """
    #     if not trade_id or not new_status:
    #         logger_object["error"].log("❌ Invalid trade_id or status provided. Skipping update.")
    #         return

    #     try:
    #         # Ensure trade exists before updating
    #         trade_record = self.mongo_client.find_one({"trade_id": trade_id})
    #         if not trade_record:
    #             logger_object["warning"].log(f"⚠️ Trade {trade_id} not found. Skipping update.")
    #             return

    #         # Update trade status in MongoDB
    #         update_query = {"$set": {"status": new_status, "last_updated": datetime.utcnow()}}
                
    #         db = self.client[mongodb_client]
    #         collection = db[collection_name]
    #         result = collection.update_one({"trade_id": trade_id}, update_query)

    #     except Exception as e:
    #         logger_object["error"].log(f"🚨 Error updating trade {trade_id} status: {e}")
    def update_trade_status(self, trade_id, new_status, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        """
        Update the status of a trade in MongoDB.

        :param trade_id: Trade ID to update
        :param new_status: New status to be set
        """
        if not trade_id or not new_status:
            logger_object["error"].log("❌ Invalid trade_id or status provided. Skipping update.")
            return

        try:
            # Ensure trade exists before updating
            trade_record = self.client[ mongodb_client ][collection_name].find_one({"trade_id": trade_id})
            if not trade_record:
                logger_object["warning"].log(f"⚠️ Trade {trade_id} not found. Skipping update.")
                return

            # Update trade status in MongoDB
            update_query = {"$set": {"status": new_status, "last_updated": datetime.utcnow()}}
                    
            db = self.client[mongodb_client]  # Access the specified database
            collection = db[collection_name]  # Access the specified collection
            result = collection.update_one({"trade_id": trade_id}, update_query)
            
            # Optional: Log the result
            if result.modified_count > 0:
                logger_object["info"].log(f"✔️ Trade {trade_id} status updated successfully.")
            else:
                logger_object["warning"].log(f"⚠️ No changes made to trade {trade_id}.")
            
        except Exception as e:
            logger_object["error"].log(f"🚨 Error updating trade {trade_id} status: {e}")


    def upsert_average_transaction(
        self,
        collection_name,
        columns,
        values,
        conflict_columns,
        side="buy",
        mongodb_client="Traderverse-Authentication"
    ):
        """Perform an upsert operation for AverageTransactions with position closing and flip logic."""
        try:
            if not columns or not values:
                raise ValueError("Columns and values cannot be empty")
            if side not in ["buy", "sell"]:
                raise ValueError("Side must be 'buy' or 'sell'")

            # Prepare values
            value_dict = {col: val for col, val in zip(columns, values[0])}
            filter_query = {key: value_dict[key] for key in conflict_columns}

            db = self.client[mongodb_client]
            collection = db[collection_name]
            existing_document = collection.find_one(filter_query)

            if existing_document:
                # Calculate new quantity
                if side == "buy":
                    new_quantity = existing_document["quantity"] + value_dict["quantity"]
                elif side == "sell":
                    new_quantity = existing_document["quantity"] - value_dict["quantity"]

                # ✅ DELETE if position fully closed
                if new_quantity == 0:
                    collection.delete_one(filter_query)
                    print(f"✅ Position closed. Document removed for {filter_query}")
                    return

                # Update average price
                if existing_document["quantity"] > 0 and new_quantity <= 0:
                    average_price = 0  # flip long → short
                elif existing_document["quantity"] < 0 and new_quantity >= 0:
                    average_price = 0  # flip short → long
                else:
                    if new_quantity > 0:
                        average_price = (
                            (existing_document["average_price"] * existing_document["quantity"] +
                            value_dict["entry_price"] * value_dict["quantity"]) / new_quantity
                        )
                    else:
                        average_price = value_dict["entry_price"]

                # Determine direction
                direction = "long" if new_quantity > 0 else "short"

                # Unrealized PnL
                if side == "buy":
                    unrealized_pnl = (value_dict["entry_price"] - existing_document["average_price"]) * new_quantity
                else:
                    unrealized_pnl = (existing_document["average_price"] - value_dict["entry_price"]) * new_quantity

                update_document = {
                    "$set": {
                        "quantity": new_quantity,
                        "average_price": average_price,
                        "entry_price": value_dict["entry_price"],
                        "direction": direction,
                        "market_value": value_dict["entry_price"] * new_quantity,
                        "trade_value": abs(average_price * new_quantity),
                        "unrealized_pnl": unrealized_pnl,
                        "updated_at": datetime.utcnow(),
                    }
                }

            else:
                # Insert new document
                if side == "sell":
                    value_dict["quantity"] = -abs(value_dict["quantity"])
                direction = "short" if side == "sell" else "long"

                value_dict.update({
                    "direction": direction,
                    "market_value": value_dict["entry_price"] * value_dict["quantity"],
                    "trade_value": abs(value_dict["entry_price"] * value_dict["quantity"]),
                    "unrealized_pnl": 0,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                })

                update_document = {"$setOnInsert": value_dict}

            result = collection.update_one(filter_query, update_document, upsert=True)
            print(f"Upsert successful for {collection_name}. Matched: {result.matched_count}, Modified: {result.modified_count}")

        except ValueError as ve:
            print(f"Validation error: {ve}")
            raise
        except Exception as e:
            print(f"Error during upsert for AverageTransactions: {e}")
            raise

    # Add cache for storing last timestamp for each symbol
    _last_timestamp = {}  # Format: {symbol: last_timestamp_ET}
