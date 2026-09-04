import os
import sys
from datetime import datetime
from pymongo import ASCENDING, errors
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.getenv("PROJECT_PATH"))

from __init__ import profile_mongodb_conventional_string

class MongodbOperation:
    def __init__(self):
        self.client = AsyncIOMotorClient(profile_mongodb_conventional_string)

    async def fetch_user_id(self, discord_id, collection, mongodb_client="Traderverse-Authentication"):
        try:
            db = self.client[mongodb_client]
            collection = db[collection]
            # Find the document asynchronously
            cursor = collection.find({"discord.id": str(discord_id)}, {"_id": 1})
            # Convert cursor to a list of results
            id_list = await cursor.to_list(length=None)
            return id_list
        
        except Exception as e:
            print("MongodbOperation-fetch_user_id :",e)
    
    async def fetch_user_portfolios(self, collection, user_id = None, portfolio_id = None, single_portfolio = False, mongodb_client="Traderverse-Authentication"):
        try:
            db = self.client[mongodb_client]
            collection = db[collection]
            if single_portfolio:
                cursor = collection.find({"_id": str(portfolio_id)})
            else:
                cursor = collection.find({"userId": str(user_id)})
            # Convert cursor to a list of results
            id_list = await cursor.to_list(length=None)
            return id_list
        
        except Exception as e:
            print("MongodbOperation-fetch_user_portfolios :",e)
    
    async def update(self, filer, operation, collection, mongodb_client="Traderverse-Authentication"):
        try:
            db = self.client[mongodb_client]
            collection = db[collection]

            collection.update_one(
                                    filer,         
                                    {"$set": operation}       
                                    )

        except Exception as e:
            print("MongodbOperation-update :",e)
    
    async def update_and_missing_key(self, filer, operation, collection, mongodb_client="Traderverse-Authentication"):
        """
        Ensures all provided keys are updated in the MongoDB document.
        - If a key exists and has a different value → Update it.
        - If a key is missing → Add it.
        - If the document does not exist → Create a new document.

        Parameters:
        - filer: Filter criteria (e.g., {"portfolio_id": "12345"})
        - operation: Dictionary of fields to update/add.
        - collection: MongoDB collection name.
        - mongodb_client: MongoDB database name (default: "Traderverse-Authentication")

        Returns:
        - Updated document confirmation message.
        """
        try:
            db = self.client[mongodb_client]
            collection = db[collection]

            # ✅ Fetch the existing document asynchronously
            existing_doc = await collection.find_one(filer)

            if existing_doc:
                # ✅ Compare values before updating to avoid unnecessary writes
                update_data = {}
                for key, value in operation.items():
                    if key not in existing_doc or existing_doc[key] != value:
                        update_data[key] = value  # ✅ Only update changed or missing values

                if update_data:  # ✅ Only update if changes exist
                    await collection.update_one(filer, {"$set": update_data})
                    print("✅ Document updated with new & changed keys.")
                else:
                    print("✅ No changes detected. Update skipped.")
            else:
                # ✅ If document doesn't exist, create a new one
                await collection.insert_one({**filer, **operation})
                print("✅ New document created with all fields.")

        except Exception as e:
            print("❌ MongoDB Operation Error:", e)





    async def insert_portfolio_data(self, user_id, collection, portfolio_id, privacy, portfolio_name, balance, currency, mongodb_client="Traderverse-Authentication"):
        try:    
            # Get the color hex code
            db = self.client[mongodb_client]

            collection = db[collection]
            document = {
                "_id": str(portfolio_id),
                "color": "#6633CC",
                "createdBy": "System",
                "createdOn": datetime.utcnow(),
                "description": "",
                "lineItems": [],
                "modifiedBy": "",
                "modifiedOn": datetime.utcnow(),
                "name": "other",
                "privacy": str(privacy),
                "text": str(portfolio_name),
                "type": "custom",
                "userId": str(user_id),
                "accountBalance": int(balance),
                "currency": currency,
                "initialBlance": int(balance),
                "realizePnl": 0,
                "unrealizePnl": 0,
                "equity": int(balance),
                "availableFund": int(balance)

            }
            document["createdAt"] = datetime.utcnow()

            # Insert the document asynchronously
            insert_result = await collection.insert_one(document)
    
        except Exception as e:
            print("MongodbOperation-insert_portfolio_data :",e)
    
    async def insert(self, document, collection, mongodb_client="Traderverse-Authentication"):

        db = self.client[mongodb_client]
        collection = db[collection]
        document["created_at"] = datetime.utcnow()
        insert_result = await collection.insert_one(document)


    # async def create_composite_index(self, collection_name, mongodb_client="Traderverse-Authentication"):
    #     try:
    #         db = self.client[mongodb_client]
    #         collection = db[collection_name]
    #         # Create a unique index on portfolio_id and asset_name
    #         await collection.create_index(
    #             [("portfolio_id", ASCENDING), ("asset_name", ASCENDING)],
    #             unique=True
    #         )
    #         print(f"Unique index on 'portfolio_id' and 'asset_name' created for {collection_name}")
    #     except errors.PyMongoError as e:
    #         print("Error creating index:", e)

    async def create_composite_index(self, collection_name, mongodb_client="Traderverse-Authentication"):
        """
        Create a unique composite index on portfolio_id and symbol.

        Parameters:
        - collection_name: The name of the collection where the index will be created.
        - mongodb_client: The MongoDB client database to connect to.
        """
        try:
            db = self.client[mongodb_client]
            collection = db[collection_name]
            
            # Create a unique composite index on portfolio_id and symbol
            index_name = await collection.create_index(
                [("portfolio_id", ASCENDING), ("symbol", ASCENDING)],
                unique=True
            )
            
            print(f"Unique composite index '{index_name}' on 'portfolio_id' and 'symbol' created for collection '{collection_name}'")
        except errors.PyMongoError as e:
            print(f"Error creating composite index for collection '{collection_name}':", e)

    
    async def delete_document(self, collection_name, filter_criteria, mongodb_client="Traderverse-Authentication"):
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
            delete_result = await collection.delete_many(filter_criteria)
            
            if delete_result.deleted_count > 0:
                print(f"Deleted {delete_result.deleted_count} document(s) from {collection_name}.")
            else:
                print("No documents matched the criteria.")
        except Exception as e:
            print(f"Error deleting document(s): {e}")
    

    async def select(self, collection_name, filter_criteria, mongodb_client="Traderverse-Authentication"):
        """
        Select documents from a MongoDB collection.
        
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
            
            # Convert cursor to a list of documents
            documents = await cursor.to_list(length=None)
            
            return documents
        except Exception as e:
            print(f"Error selecting document(s): {e}")
            return None
    

    async def return_and_delete_document(self, collection_name, filter_criteria, mongodb_client="Traderverse-Authentication"):
        """
        Delete documents from a MongoDB collection and return the deleted documents.
        
        :param collection_name: The name of the collection
        :param filter_criteria: The filter criteria to match documents
        :param mongodb_client: The name of the database
        :return: List of deleted documents
        """
        try:
            db = self.client[mongodb_client]
            collection = db[collection_name]

            # Find and store the documents to be deleted
            documents_to_delete = await collection.find(filter_criteria).to_list(length=None)

            # Proceed to delete the documents
            delete_result = await collection.delete_many(filter_criteria)
            
            if delete_result.deleted_count > 0:
                print(f"Deleted {delete_result.deleted_count} document(s) from {collection_name}.")
                return documents_to_delete
            else:
                print("No documents matched the criteria.")
                return []
        except Exception as e:
            print(f"Error deleting document(s): {e}")
            return []

    # async def upsert_average_transaction(
    #     self,
    #     collection_name,
    #     columns,
    #     values,
    #     conflict_columns,
    #     side="buy",
    #     mongodb_client="Traderverse-Authentication"
    # ):
    #     """
    #     Perform an upsert operation on the given collection for AverageTransactions.

    #     Parameters:
    #     - collection_name: Name of the MongoDB collection.
    #     - columns: List of columns to insert.
    #     - values: List of tuples containing the values to insert.
    #     - conflict_columns: List of columns to check for conflicts (e.g., symbol and portfolio_id).
    #     - side: The trade side ("buy" or "sell") to determine update logic.

    #     Raises:
    #     - Exception if the upsert operation fails.
    #     """
    #     try:
    #         if not columns or not values:
    #             raise ValueError("Columns and values cannot be empty")
    #         if side not in ["buy", "sell"]:
    #             raise ValueError("Side must be 'buy' or 'sell'")

    #         # Prepare the values for insertion
    #         value_dict = {col: val for col, val in zip(columns, values[0])}

    #         # Build the filter query using conflict columns
    #         filter_query = {key: value_dict[key] for key in conflict_columns}

    #         # Fetch the existing document (if any)
    #         db = self.client[mongodb_client]
    #         collection = db[collection_name]
    #         existing_document = await collection.find_one(filter_query)

    #         # Prepare fields for update or insert
    #         if existing_document:
    #             # Update quantity based on the side
    #             if side == "buy":
    #                 new_quantity = existing_document["quantity"] + value_dict["quantity"]
    #             else:  # side == "sell"
    #                 new_quantity = existing_document["quantity"] - value_dict["quantity"]

    #             # Update average price
    #             if new_quantity > 0:
    #                 average_price = (
    #                     (existing_document["average_price"] * existing_document["quantity"] +
    #                     value_dict["entry_price"] * value_dict["quantity"]) / new_quantity
    #                 )
    #             else:
    #                 average_price = value_dict["entry_price"]

    #             # Determine direction
    #             if new_quantity > 0:
    #                 direction = "long"
    #             elif new_quantity < 0:
    #                 direction = "short"
    #             else:
    #                 direction = "flat"

    #             # Calculate additional fields
    #             market_value = value_dict["entry_price"] * new_quantity
    #             trade_value = abs(average_price * new_quantity)
    #             if side == "buy":
    #                 unrealized_pnl = (value_dict["entry_price"] - existing_document["average_price"]) * new_quantity
    #             else:  # side == "sell"
    #                 unrealized_pnl = (existing_document["average_price"] - value_dict["entry_price"]) * new_quantity

    #             # Prepare the update document
    #             update_document = {
    #                 "$set": {
    #                     "quantity": new_quantity,
    #                     "average_price": average_price,
    #                     "entry_price": value_dict["entry_price"],
    #                     "direction": direction,
    #                     "market_value": market_value,
    #                     "trade_value": trade_value,
    #                     "unrealized_pnl": unrealized_pnl,
    #                     "updated_at": datetime.utcnow(),
    #                 }
    #             }
    #         else:
    #             # Handle initial insert
    #             if side == "sell":
    #                 value_dict["quantity"] = -abs(value_dict["quantity"])  # Ensure negative quantity for sell
    #             direction = "short" if side == "sell" else "long"

    #             market_value = value_dict["entry_price"] * value_dict["quantity"]
    #             trade_value = abs(value_dict["entry_price"] * value_dict["quantity"])
    #             unrealized_pnl = 0  # No PnL on the initial entry

    #             value_dict.update({
    #                 "direction": direction,
    #                 "market_value": market_value,
    #                 "trade_value": trade_value,
    #                 "unrealized_pnl": unrealized_pnl,
    #                 "created_at": datetime.utcnow(),
    #                 "updated_at": datetime.utcnow(),
    #             })
    #             update_document = {"$setOnInsert": value_dict}

    #         # Perform the upsert operation
    #         result = await collection.update_one(
    #             filter_query, update_document, upsert=True
    #         )

    #         # Log the result
    #         print(
    #             f"Upsert successful for {collection_name}. Matched: {result.matched_count}, Modified: {result.modified_count}"
    #         )

    #     except ValueError as ve:
    #         print(f"Validation error: {ve}")
    #         raise
    #     except Exception as e:
    #         print(f"Error during upsert for AverageTransactions: {e}")
    #         raise

    # async def upsert_average_transaction(
    #     self,
    #     collection_name,
    #     columns,
    #     values,
    #     conflict_columns,
    #     side="buy",
    #     mongodb_client="Traderverse-Authentication"
    # ):
    #     """Perform an upsert operation for AverageTransactions."""
    #     try:
    #         if not columns or not values:
    #             raise ValueError("Columns and values cannot be empty")
    #         if side not in ["buy", "sell"]:
    #             raise ValueError("Side must be 'buy' or 'sell'")

    #         # Prepare the values for insertion
    #         value_dict = {col: val for col, val in zip(columns, values[0])}

    #         # Build the filter query using conflict columns
    #         filter_query = {key: value_dict[key] for key in conflict_columns}

    #         # Fetch the existing document (if any)
    #         db = self.client[mongodb_client]
    #         collection = db[collection_name]
    #         existing_document = await collection.find_one(filter_query)

    #         # Prepare fields for update or insert
    #         if existing_document:
    #             if side == "buy":
    #                 new_quantity = existing_document["quantity"] + value_dict["quantity"]
    #             elif side == "sell":
    #                 new_quantity = existing_document["quantity"] - value_dict["quantity"]

    #             # **Handle Position Closing or Flipping**
    #             if existing_document["quantity"] > 0 and new_quantity <= 0:
    #                 average_price = 0  # Reset if flipping from long to short
    #             elif existing_document["quantity"] < 0 and new_quantity >= 0:
    #                 average_price = 0  # Reset if flipping from short to long
    #             else:
    #                 if new_quantity > 0:
    #                     average_price = (
    #                         (existing_document["average_price"] * existing_document["quantity"] +
    #                         value_dict["entry_price"] * value_dict["quantity"]) / new_quantity
    #                     )
    #                 else:
    #                     average_price = value_dict["entry_price"]

    #             # **Determine New Direction**
    #             if new_quantity > 0:
    #                 direction = "long"
    #             elif new_quantity < 0:
    #                 direction = "short"
    #             else:
    #                 direction = "flat"

    #             # **Unrealized P&L Calculation**
    #             if new_quantity == 0:
    #                 unrealized_pnl = 0
    #             else:
    #                 if side == "buy":
    #                     unrealized_pnl = (value_dict["entry_price"] - existing_document["average_price"]) * new_quantity
    #                 else:
    #                     unrealized_pnl = (existing_document["average_price"] - value_dict["entry_price"]) * new_quantity

    #             # **Prepare the update document**
    #             update_document = {
    #                 "$set": {
    #                     "quantity": new_quantity,
    #                     "average_price": average_price,
    #                     "entry_price": value_dict["entry_price"],
    #                     "direction": direction,
    #                     "market_value": value_dict["entry_price"] * new_quantity,
    #                     "trade_value": abs(average_price * new_quantity),
    #                     "unrealized_pnl": unrealized_pnl,
    #                     "updated_at": datetime.utcnow(),
    #                 }
    #             }
    #         else:
    #             # **Handle Initial Insert**
    #             if side == "sell":
    #                 value_dict["quantity"] = -abs(value_dict["quantity"])  # ✅ Ensure negative quantity for initial short
    #             direction = "short" if side == "sell" else "long"

    #             value_dict.update({
    #                 "direction": direction,
    #                 "market_value": value_dict["entry_price"] * value_dict["quantity"],
    #                 "trade_value": abs(value_dict["entry_price"] * value_dict["quantity"]),
    #                 "unrealized_pnl": 0,  # No PnL on first trade
    #                 "created_at": datetime.utcnow(),
    #                 "updated_at": datetime.utcnow(),
    #             })
    #             update_document = {"$setOnInsert": value_dict}

    #         # **Perform the upsert operation**
    #         result = await collection.update_one(filter_query, update_document, upsert=True)

    #         print(
    #             f"Upsert successful for {collection_name}. Matched: {result.matched_count}, Modified: {result.modified_count}"
    #         )

    #     except ValueError as ve:
    #         print(f"Validation error: {ve}")
    #         raise
    #     except Exception as e:
    #         print(f"Error during upsert for AverageTransactions: {e}")
    #         raise

    async def upsert_average_transaction(
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
            existing_document = await collection.find_one(filter_query)

            if existing_document:
                # Calculate new quantity
                if side == "buy":
                    new_quantity = existing_document["quantity"] + value_dict["quantity"]
                elif side == "sell":
                    new_quantity = existing_document["quantity"] - value_dict["quantity"]

                # ✅ DELETE if position fully closed
                if new_quantity == 0:
                    await collection.delete_one(filter_query)
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

            result = await collection.update_one(filter_query, update_document, upsert=True)
            print(f"Upsert successful for {collection_name}. Matched: {result.matched_count}, Modified: {result.modified_count}")

        except ValueError as ve:
            print(f"Validation error: {ve}")
            raise
        except Exception as e:
            print(f"Error during upsert for AverageTransactions: {e}")
            raise

    
    async def transactions(self, portfolio_id, mongodb_client="Traderverse-Authentication"):
        pipeline = [
            {"$match": {"portfolio_id": portfolio_id}},  # Match transactions by username
            {
                "$lookup": {
                    "from": "order_trades",         # Join with the order_trades collection
                    "localField": "trade_id",       # Field in all_transactions
                    "foreignField": "trade_id",     # Field in order_trades
                    "as": "leverage_data"           # Resulting array field for matched documents
                }
            },
            {"$unwind": {"path": "$leverage_data", "preserveNullAndEmptyArrays": True}},  # Flatten leverage_data
            {
                "$project": {
                    'trade_id': 1,
                    "transaction_id": 1,
                    "symbol": 1,
                    "quantity": 1,
                    "entry_price": 1,
                    "trade_parameter": 1,
                    "type": 1,
                    "side": 1,
                    "portfolio_id": 1,
                    "user_id": 1,
                    "created_at": 1,
                    # Project leverage_calculation fields from leverage_data
                    "borrow_money": "$leverage_data.leverage_calculation.borrow_money",
                    "market_value": "$leverage_data.leverage_calculation.market_value",
                    "paid_amount": "$leverage_data.leverage_calculation.paid_amount",
                    "daily_fee_deduction": "$leverage_data.leverage_calculation.daily_fee_deduction",
                    "total_fee_deduction": "$leverage_data.leverage_calculation.total_fee_deduction",
                    "leverage": "$leverage_data.leverage_calculation.leverage",
                    "liquidation_price": "$leverage_data.leverage_calculation.liquidation_price",
                    "commission_fee_deduction": "$leverage_data.leverage_calculation.commission_fee_deduction"
                }
            },
            {"$sort": {"created_at": -1}},  # Sort by created_at in descending order
            {"$limit": 20}  # Limit to 20 most recent records
        ]

        db = self.client[mongodb_client]    

        results = await db["all_transactions"].aggregate(pipeline).to_list(length=20)
        return results
        # for result in results:
        #     print(result)
    

    async def truncate(self, collection_name, mongodb_client="Traderverse-Authentication"):
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
            delete_result = await collection.delete_many({})
            
            if delete_result.deleted_count > 0:
                print(f"Deleted {delete_result.deleted_count} document(s) from {collection_name}.")
            else:
                print("No documents matched the criteria.")
        except Exception as e:
            print(f"Error deleting document(s): {e}")

    # async def testing_upsert_average_transaction(
    #     self,
    #     collection_name,
    #     columns,
    #     values,
    #     conflict_columns,
    #     current_market_price,
    #     side="buy",
    #     portfolio_id=None,  # User Portfolio ID for Balance Update
    #     mongodb_client="Traderverse-Authentication"
    # ):
    #     """
    #     Perform an upsert operation for AverageTransactions while managing:
    #     - Buy (Open/Extend Long or Cover Short)
    #     - Sell (Close/Reduce Long or Open/Extend Short)
    #     - Applying Realized PnL to User Balance (but NOT double-counting)

    #     Parameters:
    #     - collection_name: MongoDB collection name.
    #     - columns: List of columns for inserting.
    #     - values: List of tuples containing the values to insert.
    #     - conflict_columns: List of columns to check for conflicts (e.g., symbol, portfolio_id).
    #     - side: The trade type ("buy" or "sell").
    #     - current_market_price: The latest market price (for unrealized PnL).
    #     - portfolio_id: The user's portfolio ID (for balance updates).
    #     - mongodb_client: MongoDB connection identifier.

    #     Returns:
    #     - Dictionary containing updated quantity, realized/unrealized PnL,
    #     total trade amount, balance details, etc.
    #     """
    #     try:
    #         # Basic validation
    #         if not columns or not values:
    #             raise ValueError("Columns and values cannot be empty")
    #         if side not in ["buy", "sell"]:
    #             raise ValueError("Side must be 'buy' or 'sell'")
    #         if not portfolio_id:
    #             raise ValueError("Portfolio ID is required for balance updates")

    #         # Prepare the insertion/lookup dictionaries
    #         value_dict = {col: val for col, val in zip(columns, values[0])}
    #         filter_query = {key: value_dict[key] for key in conflict_columns}

    #         # Connect to Mongo
    #         db = self.client[mongodb_client]
    #         collection = db[collection_name]
    #         existing_document = await collection.find_one(filter_query)

    #         # Fetch user portfolio to get the current balance
    #         db_auth = self.client["Traderverse-Authentication"]
    #         portfolio_collection = db_auth["portfolio"]
    #         user_portfolio = await portfolio_collection.find_one({"_id": portfolio_id})

    #         # Current balance
    #         if user_portfolio:
    #             current_balance = user_portfolio.get("account_balance", 0)
    #         else:
    #             current_balance = 0  # fallback

    #         print("Before Trade - Account Balance:", current_balance)

    #         # Extract trade info
    #         trade_quantity = abs(value_dict["quantity"])  # always positive
    #         trade_price = value_dict["entry_price"]
    #         total_trade_amount = trade_quantity * trade_price

    #         # Initialize PnL fields & final position info
    #         realized_pnl = 0.0
    #         unrealized_pnl = 0.0
    #         new_quantity = 0
    #         new_avg_price = 0.0
    #         updated_balance = current_balance
    #         balance_change = 0.0

    #         # Helper function to compute unrealized PnL
    #         # given final quantity & average price
    #         def compute_unrealized_pnl(qty, avg_px, mkt_px):
    #             if qty > 0:  # long
    #                 return (mkt_px - avg_px) * qty
    #             elif qty < 0:  # short
    #                 return (avg_px - mkt_px) * abs(qty)
    #             else:
    #                 return 0.0

    #         # If there's an existing position
    #         if existing_document:
    #             current_quantity = existing_document["quantity"]
    #             current_avg_px = existing_document["average_price"]
    #             direction = existing_document.get("direction", "flat")

    #             # ============== HANDLE BUY SIDE ==============
    #             if side == "buy":

    #                 # CASE A: Currently LONG or flat (current_quantity >= 0)
    #                 if current_quantity >= 0:
    #                     #
    #                     #  Increase (or open) a long position
    #                     #
    #                     new_quantity = current_quantity + trade_quantity

    #                     # Weighted-average price for the new total quantity
    #                     if new_quantity > 0:
    #                         new_avg_price = (
    #                             current_avg_px * current_quantity
    #                             + trade_price * trade_quantity
    #                         ) / new_quantity
    #                     else:
    #                         # Means we ended at 0, though unlikely here.
    #                         new_avg_price = 0.0

    #                     # Realized PnL is 0 because we are not closing any short
    #                     realized_pnl = 0.0

    #                     # Balance adjustment: just pay for the buy
    #                     updated_balance -= (trade_quantity * trade_price)
    #                     balance_change = -(trade_quantity * trade_price)

    #                 # CASE B: Currently SHORT (current_quantity < 0)
    #                 else:
    #                     #
    #                     #  Part (or all) of this buy covers the short.
    #                     #  Possibly flipping to a net long if leftover.
    #                     #
    #                     short_size = abs(current_quantity)  # how many shares are short
    #                     cover_qty = min(short_size, trade_quantity)
    #                     leftover = trade_quantity - cover_qty

    #                     # Realized PnL on the covered portion
    #                     # If we are short at avg px = current_avg_px,
    #                     # realized = (avg_short_price - buy_price)*cover_qty
    #                     realized_pnl = (current_avg_px - trade_price) * cover_qty

    #                     # First, cover those shares
    #                     new_quantity = current_quantity + cover_qty  # negative + cover_qty
    #                     # new_quantity might now be 0 or still negative
    #                     # If leftover > 0, we open a new long
    #                     if new_quantity == 0 and leftover > 0:
    #                         # Now we flip to a new long with leftover shares
    #                         new_quantity = leftover
    #                         new_avg_price = trade_price
    #                     elif new_quantity < 0:
    #                         # We only partially covered, still short
    #                         # Keep the old avg price for what's left short
    #                         new_avg_price = current_avg_px
    #                     else:
    #                         # new_quantity == 0 and leftover==0 -> fully flat
    #                         # or new_quantity>0 => leftover coverage
    #                         # If leftover = 0 => flat
    #                         # If leftover = 0 but new_quantity>0 shouldn't happen,
    #                         # but let's handle carefully:
    #                         if new_quantity > 0:
    #                             # leftover buy turned us long
    #                             new_avg_price = trade_price
    #                         else:
    #                             # Exactly flat
    #                             new_avg_price = 0.0

    #                     # Balance adjustment: 
    #                     # We pay for the entire trade_qty * trade_price
    #                     # (the realized PnL is NOT added or subtracted a second time).
    #                     updated_balance -= (trade_quantity * trade_price)
    #                     balance_change = -(trade_quantity * trade_price)

    #                 # Recompute direction post‐trade
    #                 if new_quantity > 0:
    #                     direction = "long"
    #                 elif new_quantity < 0:
    #                     direction = "short"
    #                 else:
    #                     direction = "flat"

    #             # ============== HANDLE SELL SIDE ==============
    #             else:  # side == "sell"

    #                 # CASE C: Currently LONG (current_quantity > 0)
    #                 if current_quantity > 0:
    #                     if trade_quantity <= current_quantity:
    #                         # Partial or full close of the long
    #                         close_qty = trade_quantity
    #                         leftover_qty = current_quantity - close_qty

    #                         # Realized PnL on the portion we sell
    #                         realized_pnl = (trade_price - current_avg_px) * close_qty

    #                         new_quantity = leftover_qty
    #                         if new_quantity > 0:
    #                             # still long
    #                             new_avg_price = current_avg_px
    #                         else:
    #                             # fully flat
    #                             new_avg_price = 0.0

    #                         # Increase balance by the proceeds of the sale
    #                         updated_balance += (close_qty * trade_price)
    #                         balance_change = (close_qty * trade_price)

    #                     else:
    #                         # We are selling more than we hold => flip to short
    #                         close_qty = current_quantity
    #                         leftover_qty = trade_quantity - close_qty

    #                         # Realized PnL on closing the long portion
    #                         realized_pnl = (trade_price - current_avg_px) * close_qty

    #                         # Now we open a short with leftover_qty
    #                         new_quantity = -leftover_qty
    #                         new_avg_price = trade_price

    #                         # Proceeds from selling all 'trade_quantity'
    #                         updated_balance += (trade_quantity * trade_price)
    #                         balance_change = (trade_quantity * trade_price)

    #                 # CASE D: Currently SHORT or flat (current_quantity <= 0)
    #                 else:
    #                     if current_quantity < 0:
    #                         # Already short, adding more to the short
    #                         # or partially covering + re-shorting
    #                         if trade_quantity > 0:
    #                             # effectively we are short-selling more shares
    #                             new_quantity = current_quantity - trade_quantity

    #                             # Weighted average if we add to an existing short
    #                             total_short = abs(current_quantity) + trade_quantity
    #                             new_avg_price = (
    #                                 current_avg_px * abs(current_quantity)
    #                                 + trade_price * trade_quantity
    #                             ) / total_short

    #                             # Realized PnL = 0 here, because we didn't close any short
    #                             realized_pnl = 0.0

    #                             # Increase balance by proceeds of short-sell
    #                             updated_balance += (trade_quantity * trade_price)
    #                             balance_change = (trade_quantity * trade_price)
    #                         else:
    #                             # If trade_quantity == 0, not realistic, but fallback
    #                             new_quantity = current_quantity
    #                             new_avg_price = current_avg_px

    #                     else:
    #                         # current_quantity == 0 (flat)
    #                         # So we are simply opening a short
    #                         new_quantity = -trade_quantity
    #                         new_avg_price = trade_price
    #                         realized_pnl = 0.0

    #                         # Add short-sell proceeds
    #                         updated_balance += total_trade_amount
    #                         balance_change = total_trade_amount

    #                 # Recompute direction
    #                 if new_quantity > 0:
    #                     direction = "long"
    #                 elif new_quantity < 0:
    #                     direction = "short"
    #                 else:
    #                     direction = "flat"

    #         # ============== NO EXISTING DOCUMENT ==============
    #         else:
    #             #
    #             # First position: simple open of a long or short
    #             #
    #             if side == "buy":
    #                 new_quantity = trade_quantity
    #                 new_avg_price = trade_price
    #                 direction = "long"

    #                 # Deduct cost from balance
    #                 updated_balance -= total_trade_amount
    #                 balance_change = -total_trade_amount
    #                 realized_pnl = 0.0

    #             else:  # side == "sell"
    #                 new_quantity = -trade_quantity
    #                 new_avg_price = trade_price
    #                 direction = "short"

    #                 # Add short-sell proceeds
    #                 updated_balance += total_trade_amount
    #                 balance_change = total_trade_amount
    #                 realized_pnl = 0.0

    #         # ============== Compute Unrealized PnL ==============
    #         unrealized_pnl = compute_unrealized_pnl(new_quantity, new_avg_price, current_market_price)

    #         # ============== Prepare DB update ==============
    #         # If we are flat, market_value = 0, etc.
    #         market_value = abs(new_quantity) * current_market_price
    #         trade_value = abs(new_quantity) * new_avg_price

    #         print("After Trade - Account Balance:", updated_balance)
    #         print("Balance Change:", balance_change)

    #         # (Optionally) Update user balance in portfolio
    #         # await portfolio_collection.update_one(
    #         #     {"_id": portfolio_id},
    #         #     {"$set": {"account_balance": updated_balance}}
    #         # )

    #         update_document = {
    #             "$set": {
    #                 "quantity": new_quantity,
    #                 "average_price": new_avg_price,
    #                 "entry_price": trade_price,    # last trade price used
    #                 "direction": direction,
    #                 "market_value": market_value,
    #                 "trade_value": trade_value,
    #                 "unrealized_pnl": unrealized_pnl,
    #                 "realized_pnl": realized_pnl,
    #                 "updated_at": datetime.utcnow(),
    #             }
    #         }

    #         # Perform the upsert
    #         result = await collection.update_one(filter_query, update_document, upsert=True)
    #         print(f"Upsert successful for {collection_name}. "
    #             f"Matched: {result.matched_count}, Modified: {result.modified_count}")

    #         return {
    #             "quantity": new_quantity,
    #             "average_price": new_avg_price,
    #             "direction": direction,
    #             "realized_pnl": realized_pnl,
    #             "unrealized_pnl": unrealized_pnl,
    #             "total_trade_amount": total_trade_amount,
    #             "balance_before_trade": current_balance,
    #             "balance_after_trade": updated_balance,
    #             "balance_change": balance_change
    #         }

    #     except ValueError as ve:
    #         print(f"Validation error: {ve}")
    #         raise
    #     except Exception as e:
    #         print(f"Error during upsert for AverageTransactions: {e}")
    #         raise