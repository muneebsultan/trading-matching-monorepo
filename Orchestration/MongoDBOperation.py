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
