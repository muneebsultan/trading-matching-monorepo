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
    