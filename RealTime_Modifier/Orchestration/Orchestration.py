import os
import uuid
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

from Core.__init__ import logger_object
from Core.general import DatabaseManager  
mo = DatabaseManager()

class CancelTradeOrchestrator():
    def __init__(self):
        pass

    def cancel_trade_update_alltransactions(self, partial_entry):
        try:
            """Update transaction records."""
            transaction_trade_id_1 = str(uuid.uuid4())
            transaction_trade_id_2 = str(uuid.uuid4())

            document_1 = {
                'transaction_id': transaction_trade_id_1,
                'username': partial_entry['username'],
                'symbol': partial_entry['symbol'],
                'quantity': int(partial_entry['remaining_quantity']),
                'entry_price': partial_entry['entry_price'],
                'trade_parameter': 'Cancel',
                'type': partial_entry['order_type'],
                'side': partial_entry['side'],
                'portfolio_id': partial_entry['portfolio_id'],
                'user_id': partial_entry['user_id'],  # Adjust interaction context if required
                'trade_id': partial_entry['trade_id']
            }

            document_2 = {
                'transaction_id': transaction_trade_id_2,
                'username': partial_entry['username'],
                'symbol': partial_entry['symbol'],
                'quantity': int(partial_entry['volume']),
                'entry_price': partial_entry['entry_price'],
                'trade_parameter': 'Filled',
                'type': partial_entry['order_type'],
                'side': partial_entry['side'],
                'portfolio_id': partial_entry['portfolio_id'],
                'user_id': partial_entry['user_id'],  # Adjust interaction context if required
                'trade_id': partial_entry['trade_id']
            }

            mo.insert(document=document_1, collection=transaction_table, mongodb_client="Traderverse-Authentication")
            mo.insert(document=document_2, collection=transaction_table, mongodb_client="Traderverse-Authentication")
            logger_object['success'].log(f"✅ Successfully insert alltransactions record for trade_id: {partial_entry['trade_id']} with transaction_id 1 : {transaction_trade_id_1} & transaction_trade_id_2 : {transaction_trade_id_2}")
        
        except Exception as e:
            logger_object["error"].log(f"PartialActionView_update_transactions: 🚨 Error in insertion all_transacation: {e}")
    
    def update_order_trade_table(self, partial_entry, trade_id):
        try:
            logger_object['success'].log(f"Updating trade {trade_id} with data: {partial_entry}")
            mo.update(
                collection=trade_table, 
                filer={"trade_id": str(trade_id)}, 
                operation={
                    "status": "Filled",
                    "quantity": partial_entry['volume'],
                    "entry_price": partial_entry['entry_price'],
                    "remaining_quantity": 0,
                    "last_updated": datetime.utcnow()
                }
            )
            logger_object['success'].log(f"✅ Successfully update order_trades record for trade_id: {partial_entry['trade_id']}")

        except Exception as e:
            logger_object["error"].log(f"update_order_trade_table: 🚨 Error in updating order_trade: {e} with trade_id: {trade_id}")
    
    def delete_old_transaction(self, trade_id):
        try:
            mo.delete_document(
                        collection_name=transaction_table, filter_criteria={"trade_id": str(trade_id)}, mongodb_client="Traderverse-Authentication"
                    )
            logger_object['success'].log(f"✅ Successfully delete alltransaction record for trade_id: {trade_id}")
        
        except Exception as e:
            logger_object["error"].log(f"delete_old_transaction: 🚨 Error in deletion in all_transaction: {e} with trade_id: {trade_id}")


class CancelPendingTrade():
    def __init__(self, trade_id):
        self.trade_id = trade_id

    def delete_trade_from_order_trades(self, collection_name = trade_table):
        try:
            mo.delete_document(collection_name=collection_name, filter_criteria={'trade_id':str(self.trade_id)}, mongodb_client="Traderverse-Authentication")
            logger_object['success'].log(f"✅ Successfully delete data from {collection_name} table for trade_id: {self.trade_id}")
        except Exception as e:
            logger_object["error"].log(f"CancelPendingTrade-delete_trade_from_order_trades: 🚨 Error in deletion in {collection_name}: {e} with trade_id: {self.trade_id}")


    def add_record_in_transaction_table(self, pending_trade, collection_name=transaction_table):
        try:
            transaction_id = str(uuid.uuid4())
            document = {'transaction_id': transaction_id, 'username':pending_trade.get("username", None), 'symbol':pending_trade.get("symbol"), 'quantity':pending_trade.get("quantity"), 'entry_price':pending_trade.get("entry_price"), 'entry_date':datetime.utcnow(), 'trade_parameter':'Cancel', 'order_type':pending_trade.get("order_type"), 'side':pending_trade.get("side"), 'portfolio_id':pending_trade.get("portfolio_id"), 'user_id': pending_trade.get("user_id"), 'trade_id':self.trade_id}
            mo.insert(document=document, collection=collection_name, mongodb_client="Traderverse-Authentication")
            logger_object['success'].log(f"✅ Successfully insert data in {collection_name} table for trade_id: {self.trade_id} and trasaction_id: {transaction_id}")
        except Exception as e:
            logger_object["error"].log(f"CancelPendingTrade-add_record_in_transaction_table: 🚨 Error in insert in {collection_name}: {e} with trade_id: {self.trade_id}")