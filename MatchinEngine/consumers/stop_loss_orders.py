import json
import os
import time
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from bson import ObjectId 

from Core.nats_price_service import NATSPriceService 
from Core.handle_portfolio_balance_and_fund import HandlePortfolioBalanceAndFund
from Core.general import DatabaseManager
from Core.__init__ import logger_object

load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

# Kafka Configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")
TOPIC = "stop_orders"
GROUP_ID = "stop_order_group"


class StopLossOrderProcessor:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.processed_trades = set()
        self.price_service = NATSPriceService()

        self.consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest"  # Ensure processing from the latest uncommitted message
        )

    def process_stop_loss_order(self, event, side):
        """
        Process Stop-Loss or Take-Profit directly based on actual holdings.
        Assumes:
        - Locking and order filtering is handled externally
        - Only one SL/TP is executing for a given user+symbol at a time
        """
        trade_id = event.get("trade_id")
        user_id = event.get("user_id")
        portfolio_id = event.get("portfolio_id")
        symbol = event.get("symbol", "")
        trigger_type = event.get("trigger_type", "").lower()
        user_trade_time = event.get("user_trade_time")
        order_type = event.get("order_type")
        action = "Buy" if side == "buy" else "Sell"
        symbol = symbol.upper()
        total_transaction_quantity = 0

        if not all([trade_id, portfolio_id, symbol]):
            logger_object['error'].log(f"🚨 Missing required fields in event: {event}")
            return

        logger_object['info'].log(f"🔴 Executing {trigger_type} {action} Order {trade_id} for {symbol}...")

        if trigger_type.lower() == 'liquidation':
            self.db_manager.save_liquidation_record(event)

        # Step 1: Get current holding
        user_holdings = self.user_holdings(portfolio_id=portfolio_id, symbol=symbol)
        if user_holdings is None:
            logger_object['error'].log(f"🔴 Holding not found on the behalf of portfolio_id {portfolio_id} and symbol {symbol}")
            return ("User holding not found")

        holding_quantity = user_holdings[0].get("quantity",0)
        if holding_quantity == 0:
            self.update_trade_status(trade_id=trade_id, status='Inactive', collection_name=trade_table)
            self.add_record_in_transaction(price=event.get("entry_price"), order_type=order_type, user_id=user_id, portfolio_id=portfolio_id, symbol=symbol.upper(), trade_id=trade_id, status = 'Inactive')
            logger_object['info'].log(f"🔴 Holding not found on the behalf of portfolio_id {portfolio_id} and symbol {symbol}, change status to Inactive")
            return{
                f"🔴 Holding not found on the behalf of portfolio_id {portfolio_id} and symbol {symbol}, change status to Inactive"
            }


        remaining_quantity = holding_quantity
        count = 0
        max_iterations = 100

        while True:
            if count >= max_iterations:
                logger_object['error'].log(f"⛔ Max iterations hit for trade {trade_id}. Possible logic loop.")
                break

            user_holding_symbol_wise = self.user_holdings(portfolio_id=portfolio_id, symbol=symbol)
            if user_holding_symbol_wise is None:
                logger_object['error'].log(f"🔴 Holding not found on the behalf of portfolio_id {portfolio_id} and symbol {symbol}")
                break
            
            holding_side = user_holding_symbol_wise[0].get("direction")
            real_time_holding_quantity = user_holding_symbol_wise[0].get("quantity",0)
            
            if real_time_holding_quantity <= 0:
                logger_object['error'].log(f"{count} ⚠️ No market liquidity to execute {action} order for {symbol}")
                self.update_trade_status(trade_id=trade_id, status="Filled")
                self.db_manager.update(
                    collection=transaction_table,
                    filer={"trade_id": str(trade_id)},
                    operation={
                        "quantity": total_transaction_quantity
                    }
                )
                break

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
                logger_object['info'].log(f"{count} ❌ Order {trade_id} No market price data for {symbol}")
                continue
                
            if stock_data:
                logger_object['info'].log(f"{count} New stock price {stock_data}")

            _, _, ask_price, ask_volume, bid_price, bid_volume= stock_data
            market_price = ask_price if side == "buy" else bid_price
            available_volume = ask_volume if side == "buy" else bid_volume

            transact_quantity = min(real_time_holding_quantity, available_volume)
            remaining_quantity -= transact_quantity
            total_transaction_quantity += transact_quantity
            
            columns = ["entry_price", "quantity", "symbol", "portfolio_id", "average_price"]
            values= [[market_price, transact_quantity, symbol, portfolio_id, market_price]]
            conflict_columns=["symbol", "portfolio_id"]
            side = self.determine_exit_side(holding_side=holding_side, exit_type=order_type)
            self.db_manager.upsert_average_transaction(side=side, columns=columns, values=values, conflict_columns=conflict_columns, collection_name=avg_transaction_table)
            self.update_trades_execution_and_status(trade_id=trade_id, remaining_quantity=remaining_quantity, executed_quantity=transact_quantity, price=market_price, symbol=symbol, user_entry_price=event.get("entry_price"), order_type=order_type, user_id=user_id, portfolio_id=portfolio_id, total_transaction_quantity=total_transaction_quantity, collection_name=trade_table, mongodb_client="Traderverse-Authentication")

            # 🔁 Async balance update
            handler = HandlePortfolioBalanceAndFund(
                portfolio_id=portfolio_id,
                asset_name=None,
                price=market_price,
                quantity=transact_quantity,
                side=order_type,
                symbol=symbol,
                trade_id=trade_id,
                user_id=user_id,
                close_price=market_price
            )

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(handler.HandlePortfolioHub())
                else:
                    loop.run_until_complete(handler.HandlePortfolioHub())
            except RuntimeError:
                asyncio.run(handler.HandlePortfolioHub())
            except Exception as e:
                logger_object['error'].log(f"❌ Error running HandlePortfolioHub: {e}")

            logger_object['success'].log(f"{count} portfolio_id {portfolio_id}, symbol {symbol}, new_holding: {real_time_holding_quantity}")
            
            logger_object['info'].log(f"iteration: {count} completed, symbol {symbol}, new_holding: {real_time_holding_quantity}")
            # holding_quantity = new_holding
            count += 1
    
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
    
    def add_record_in_transaction(self, price, order_type, user_id, portfolio_id, symbol, trade_id, status, total_transaction_quantity=None):
            try:

                order_place_document_transaction = {
                    'transaction_id': ObjectId(), 
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'trade_parameter': status,
                    'entry_price': price,
                    'price': price,
                    'portfolio_id': portfolio_id,
                    'user_id':  user_id,
                    'order_type': order_type,
                    'quantity': total_transaction_quantity,
                    'entry_date': datetime.now(timezone.utc)
                }

                self.db_manager.insert(document=order_place_document_transaction, collection=transaction_table, mongodb_client="Traderverse-Authentication")
                logger_object["success"].log(f"Success: successfully add document in transaction table with trade_id: {trade_id}")
            
            except Exception as e:
                logger_object["error"].log(f"Error: add_record_in_transaction: Face error {e}")

    def update_trades_execution_and_status(self, trade_id, remaining_quantity, executed_quantity, price, symbol, user_entry_price, user_id, portfolio_id, order_type, total_transaction_quantity, collection_name=trade_table, mongodb_client="Traderverse-Authentication"):
        try:
            execution_record = {
                "executed_quantity": executed_quantity,
                "remaining_quantity": remaining_quantity,
                "price": price,
                "timestamp": datetime.utcnow()
            }

            status = "Filled" if remaining_quantity == 0 else "Partial Filled"

            self.db_manager.set_and_push_mongo_fields(
                filter_query={"trade_id": str(trade_id)},
                set_fields={
                    "status": status,
                    "remaining_quantity": remaining_quantity
                },
                push_fields={"executions": execution_record},
                collection=collection_name,
                mongodb_client=mongodb_client
            )
        
            if remaining_quantity <= 0:
            #         self.update_trade_status(trade_id=trade_id, status="Filled", collection_name=transaction_table, transaction=True)
            #         logger_object['info'].log(f"✅ Remaining quantity is equal to zero status been change to Filled in {transaction_table} for trade_id {trade_id}")
                self.add_record_in_transaction(price=user_entry_price, order_type=order_type, user_id=user_id, portfolio_id=portfolio_id, symbol=symbol.upper(), trade_id=trade_id, total_transaction_quantity=total_transaction_quantity, status = 'Filled')
                self.db_manager.update(
                    collection=transaction_table,
                    filer={"trade_id": str(trade_id)},
                    operation={
                        "quantity": total_transaction_quantity
                    }
                )

        except Exception as e:
            logger_object['error'].log(f"update_trades_execution_and_status: error in getting quantity {e}")    
    
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
        

    def consume_kafka_messages(self):
        """
        Continuously listens for stop-loss & take-profit orders.
        """
        logger_object['info'].log(f"🎧 Listening for messages on Kafka topic: {TOPIC}...")

        for message in self.consumer:
            
            logger_object["info"].log(f"📩 Received Kafka Message {message}")
            try:
                event = message.value
                side = event.get("side", "").lower()

                if side == "buy":
                    self.process_stop_loss_order(event, "buy")
                elif side == "sell":
                    self.process_stop_loss_order(event, "sell")
                else:
                    logger_object['warning'].log(f"⚠️ Unknown order type received: {event}")

            except Exception:
                import traceback
                logger_object['error'].log("🚨 Error processing Kafka message:\n" + traceback.format_exc())



    def start(self):
        """ Keep Kafka consumer running 24/7 """
        while True:
            try:
                self.consume_kafka_messages()
            except Exception as e:
                logger_object['error'].log(f"🚨 Unexpected Error: {e}. Restarting consumer...")
                time.sleep(5)  # Short delay before restarting

if __name__ == "__main__":
    processor = StopLossOrderProcessor()
    processor.start()