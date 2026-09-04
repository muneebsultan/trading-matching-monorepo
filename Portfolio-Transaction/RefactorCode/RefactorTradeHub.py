import json

from __init__ import logger_object
from RefactorCode.RefactorOMS import OrderManagementSystem, Order, OrderValidator, PortfolioManager, ModifyInput, CancelInput, ValidateModifyCancelInput, TradeAndTransactionRecord, TpAndSlRecord
from RefactorCode.MatchingEngineAPI import MatchingEngine, MatchingEngineCancelModifyTrade
from Orchestration.MongoDBOperation import MongodbOperation
from RefactorCode.RedisData import get_latest_stock_price

oms = OrderManagementSystem()
mo = MongodbOperation()

class TradeExecution():
    def __init__(self):
        pass

    async def show_trade_direction(self, user_id, symbol, quantity, side, portfolio_id, stockId, asset_type, createdBy, price=None, order_type="market", tif_type="GTC", stop_loss=None, take_profit=None):
        try:
            side = side.lower()

            # 1. Fetch latest close price
            close_price = get_latest_stock_price(symbol)
            
            if close_price is None:
                return {'error': 'Failed to fetch live price data', 'status': 0}

            close_price = json.loads(close_price)

            # If no price provided for market order, use live market price
            if price is None:
                price = close_price.get("price",0)

            # 6. Create Order object
            new_order = Order(
                user_id=user_id,
                symbol=symbol,
                quantity=quantity,
                price=price,
                side=side,
                portfolio_id=portfolio_id,
                order_type=order_type,
                tif_type=tif_type,
                stop_loss=stop_loss,
                take_profit=take_profit,
                stockId=stockId,
                asset_name=asset_type,
                createdBy=createdBy
            )
            
            #getting direction from holdings
            try:    
                direction = None

                if new_order.portfolio_id and new_order.symbol:
                    filter_criteria = {
                        'portfolio_id': str(new_order.portfolio_id),
                        'symbol': new_order.symbol
                    }
                    symbols_holder_data = await mo.select(
                        collection_name="AverageTransactions",
                        filter_criteria=filter_criteria
                    )

                    if symbols_holder_data:
                        direction = symbols_holder_data[0].get('direction')

            except Exception as e:
                logger_object['error'].log(
                    f"LimitTrade_show_trade_direction-holding_direction: Error fetching holdings - {e}"
                    )
                direction = None


            # 7. Validate order
            order_validator = OrderValidator(
                user_id=new_order.user_id,
                symbol=new_order.symbol,
                quantity=new_order.quantity,
                price=new_order.price,
                side=new_order.side,
                portfolio_id=new_order.portfolio_id,
                order_type=new_order.order_type,
                tif_type=new_order.tif_type,
                asset_name=new_order.asset_name,
                stop_loss=new_order.stop_loss,
                take_profit=new_order.take_profit,
                close_price=new_order.price,
                direction=direction
            )
            order_validator.validate_order()


            # # 2. Fetch leverage info
            leverage_data = await mo.select(
                collection_name="discord_leverages",
                filter_criteria={'portfolios_id': str(new_order.portfolio_id), 'asset_name': new_order.asset_name}
            )

            inc_leverage_trades, leverage_ratio = await oms.calculate_trade_calculation(
                leverage_data, entry_price=new_order.price, quantity=new_order.quantity
            )

            # 4. Calculate liquidation price
            liquidation_price = oms.calculate_liquidation_price(
                entry_price=float(new_order.price),
                leverage_ratio=leverage_ratio,
                side=new_order.side
            )

            try:
                # save data to database
                trades_transaction_records = TradeAndTransactionRecord(
                    trade_id=new_order.trade_id,
                    symbol=new_order.symbol,
                    side=new_order.side,
                    order_type=new_order.order_type,
                    quantity=new_order.quantity,
                    portfolio_id=new_order.portfolio_id,
                    user_id=new_order.user_id,
                    liquidation_price=liquidation_price,
                    price=new_order.price,
                    stop_loss=new_order.stop_loss,
                    take_profit=new_order.take_profit,
                    tif_type=new_order.tif_type,
                    createdBy=new_order.createdBy,
                    stockId=new_order.stockId,
                    asset_type=new_order.asset_name
                )
                await trades_transaction_records.add_record_in_trade_and_transaction()
            
            except Exception as e:
                logger_object['error'].log(
                    f"LimitTrade_show_trade_direction-trade_place: Error in trade place - {e}"
                    )
                return {
                    'error': f'Error in trade placement: {e}',
                    'status': 0
                }
                            
            # Send to Matching Engine
            matching_engine = MatchingEngine(
                user_id=new_order.user_id,
                order_type=new_order.order_type,
                side=new_order.side,
                symbol=new_order.symbol,
                quantity=new_order.quantity,
                price=new_order.price,
                trade_id=new_order.trade_id,
                portfolio_id=new_order.portfolio_id,
                liquidation_price=liquidation_price,
                stop_loss=new_order.stop_loss,
                take_profit=new_order.take_profit,
                tif_type=new_order.tif_type,
                created_by=new_order.createdBy,
                stockId=new_order.stockId,
                asset_type=new_order.asset_name
            )
            matching_engine.publish_order()

            return self._send_confirmation_embed(trade_date=new_order.trade_date, quantity=new_order.quantity, entry_price=new_order.price, symbol=new_order.symbol, trade_id=new_order.trade_id)

        except Exception as e:
            logger_object['error'].log(f"LimitTrade_show_trade_direction: {e}")
            return {'result': f'Error: {e}', 'status': 0}  
    
    def _reject_trade(self):
        return {
            "result": "❌ Your trade has been rejected due to insufficient balance.",
            "status": 0
        }


    def _send_confirmation_embed(self, trade_date, quantity, entry_price, symbol, trade_id):
        try:
            return {"success":f"Your trade has been successfully recorded with the following details:\n **🔹 Symbol** {symbol}\n **📅 Trade Date** {trade_date} EST \n **🔢 Quantity** {quantity} \n **💰 Entry Price**  ${float(entry_price):,.2f}", "status":1, "tradeId": trade_id}

        except Exception as e:
            logger_object['error'].log(f"LimitAdvanceTradeModal-_send_confirmation_embed: {e}")  

class TakeProfitOrStopLoss():
    def __init__(self):
        pass

    async def operation_execute(self, user_id, symbol, portfolio_id, price, order_type, stockId, createdBy):
        try:
            new_order = Order(
                user_id=user_id,
                symbol=symbol,
                price=price,
                portfolio_id=portfolio_id,
                order_type=order_type, # Can be stoploss or takeprofitt
                stockId=stockId,
                createdBy=createdBy
            )
            if new_order.portfolio_id is not None and new_order.symbol is not None:
                symbols_holder_data = await mo.select(collection_name="AverageTransactions", filter_criteria={'portfolio_id': str(new_order.portfolio_id), "symbol": new_order.symbol()})
                
                if symbols_holder_data:
                    side = self.determine_exit_side(holding_side=symbols_holder_data[0]['direction'], exit_type=order_type)
                else: 
                    return {"error": "Holding not availabe", "status":0}
            # validate order parameters
            try:
                order_validator = OrderValidator(
                    user_id=new_order.user_id,
                    quantity=symbols_holder_data[0]['quantity'],
                    symbol=new_order.symbol,
                    price=new_order.price,
                    side=side,
                    portfolio_id=new_order.portfolio_id,
                    order_type=new_order.order_type,
                    tif_type=new_order.tif_type,
                    asset_name=new_order.asset_name,
                    close_price=0, #in takeprofit&stoploss no need of closeprice
                    direction=symbols_holder_data[0]['direction']
                )
                order_validator.validate_order()
            except ValueError as e:
                return str(e)

                        # save data to database
            
            # validate takeprofit and stoploss parameters
            try:
                order_validator.takeprofit_stoploss_validator(direction=symbols_holder_data[0]['direction'], price=new_order.price, holding_price=symbols_holder_data[0]['average_price'], holding_quantity=symbols_holder_data[0]['quantity'])
            except ValueError as e:
                return str(e)
            
            # add record of tp or sl in transaction and order_trades table
            trades_transaction_records = TpAndSlRecord(
                trade_id=new_order.trade_id,
                symbol=new_order.symbol,
                side=side,
                order_type=new_order.order_type,
                portfolio_id=new_order.portfolio_id,
                user_id=new_order.user_id,
                price=new_order.price,
                createdBy=new_order.createdBy,
                stockId=new_order.stockId,
                asset_type=new_order.asset_name,
                direction=symbols_holder_data[0]['direction']
            )
            await trades_transaction_records.add_record_in_tp_and_sl()

            #send to real time matching engine
            matching_engine = MatchingEngine(user_id=new_order.user_id, order_type=new_order.order_type, side=side, symbol=new_order.symbol, quantity=new_order.quantity, price=new_order.price, trade_id=new_order.trade_id, portfolio_id=new_order.portfolio_id, stockId=new_order.stockId, created_by=createdBy, tif_type=None, asset_type="stock")
            matching_engine.take_profit_stop_loss(direction=symbols_holder_data[0]['direction'])

            return self._send_confirmation_embed(order_type=new_order.order_type, symbol=new_order.symbol, quantity=new_order.quantity, price=new_order.price, trade_id=new_order.trade_id)
                     
        except Exception as e:
            logger_object['error'].log(f"TakeProfitOrStopLoss-operation_execute: {e}")  

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
        
    def _send_confirmation_embed(self, order_type, symbol, quantity, price, trade_id):
        try:
            return {"success":f"Your {order_type} order has been successfully recorded for symbol: {symbol} at a price of {price}.", "status":1, "tradeId": trade_id}

        except Exception as e:
            logger_object['error'].log(f"LimitAdvanceTradeModal-_send_confirmation_embed: {e}") 

class ModifyTrade():
    def __init__(self, trade_id, update_price):
        self.trade_id = trade_id
        self.update_price = update_price
    

    async def operation_perform(self):
        try:
            new_modification = ModifyInput(trade_id=self.trade_id, update_price=self.update_price)

            # validate order parameters
            try:
                modification_validator = ValidateModifyCancelInput(trade_id=new_modification.trade_id, update_price=new_modification.update_price)
                trade_data = await mo.select(collection_name="order_trades", filter_criteria={'trade_id': str(new_modification.trade_id)})
                modification_validator.edit_trades(trade_data=trade_data)

                portfolio_id = trade_data[0].get('portfolio_id', 0)
                if not portfolio_id:
                    raise ValueError({"Message":"Error: No Portfolio ID found.", "status":0})
                
                quantity = trade_data[0].get('quantity', 0)
                if quantity < 0:
                    raise ValueError({"Message":"Quantity must be positive.", "status":0})
                
            except ValueError as e:
                return str(e)
            
            inc_leverage_trades, leverage_ratio = await oms.calculate_trade_calculation(leverage_data=None, entry_price=new_modification.update_price, quantity=quantity)
                
            available_funds, account_balance = await self.get_user_portfolio(portfolio_id=portfolio_id)

            #calculate liquidation price
            liquidation_price = oms.calculate_liquidation_price(entry_price = float(new_modification.update_price))
            
            #if fund is not enough reject trade
            if inc_leverage_trades['total_deductions'] >= available_funds:
                    return {'Message': self._reject_trade(), 'status': 0}
                
            await self.update_portfolio(trade_data=trade_data, account_balance=account_balance, available_funds=available_funds, updated_price=new_modification.update_price, inc_leverage_trades=inc_leverage_trades)
            
            matching_engine = MatchingEngineCancelModifyTrade(trade_id=new_modification.trade_id, update_price=new_modification.update_price, liquidation_price=liquidation_price)
            matching_engine.ModifyTrade()

            return self.confirmation_message()

        except Exception as e:
            logger_object['error'].log(f"ModifyTrade-operation_perform: {e}") 

    
    async def get_user_portfolio(self, portfolio_id):
        try:
            portfolio_data = await mo.fetch_user_portfolios(collection="portfolio", portfolio_id=str(portfolio_id), single_portfolio=True)
            availableFund = portfolio_data[0].get('availableFund', 0)
            accountBalance = portfolio_data[0].get('accountBalance', 0)

            return availableFund, accountBalance
        except Exception as e:
            logger_object['error'].log(f"ModifyTrade-get_user_portfolio: {e}") 
    
    async def update_portfolio(self, trade_data, account_balance, available_funds, updated_price, inc_leverage_trades):
        try:
            trade_id = trade_data[0].get('trade_id', "abcd")
            trade_ledger = await mo.select(collection_name="trade_ledger", filter_criteria={'trade_id': str(trade_id)})
            side = trade_data[0].get('side', 'flat')
            if trade_ledger:
                get_deduct_balance = trade_ledger[0].get('update_balance', 0)

                if side == 'buy':
                    available_funds += get_deduct_balance
                elif side == 'sell':  
                    available_funds -= get_deduct_balance

            # stockId = trade_data[0].get('stockId', "AZABCDERTR")
            symbol = trade_data[0].get('symbol', "Unknown")

            # 1. Fetch latest close price
            c_price = get_latest_stock_price(symbol)
            c_price = json.loads(c_price)
            
            if c_price is None:
                return {'result': 'Failed to fetch live price data', 'status': 0}
            
            close_price = c_price.get("price",0)
            portfolio_id = trade_data[0].get('portfolio_id', 0)
            user_id = trade_data[0].get('user_id', 0)
            quantity = trade_data[0].get('quantity', 0)

            
            # handle balance deduction and addition
            user_portfolio = PortfolioManager(user_id=user_id, portfolio_id=portfolio_id)
            await user_portfolio.update_balance_and_fund(side, account_balance, available_funds, inc_leverage_trades, current_quantity=quantity, close_price = close_price, entry_price=updated_price, portfolio_id=portfolio_id, trade_id=trade_id)

        except Exception as e:
            logger_object['error'].log(f"ModifyTrade-update_portfolio: {e}") 

    def _reject_trade(self):
        return{"Message":"Your trade has been rejected due to insufficient balance", "status": 0}
    
    def confirmation_message(self):
        return {"Message":"Your pending trade successfully edit", "status":1}

class CancelTrade:
    def __init__(self, trade_id):
        self.trade_id = trade_id
    
    async def operation_perform(self):
        try:
            new_modification = CancelInput(trade_id=self.trade_id)
            modification_validator = ValidateModifyCancelInput(trade_id=new_modification.trade_id)
            
            trade_data = await mo.select(collection_name="order_trades", filter_criteria={'trade_id': str(new_modification.trade_id)})
            modification_validator.cancel_trades(trade_data=trade_data) 
            
            if not trade_data:
                return {"Message": "Trade not found", "status": 0}
            
            status = trade_data[0].get('status')
            if status not in {'Pending', 'Partial Filled'}:
                return {"Message": "Status must be 'Pending' or 'Partial Filled'", "status": 0}
            
            portfolio_id = trade_data[0].get('portfolio_id')
            if not portfolio_id:
                return {"Message": "Error: No Portfolio ID found.", "status": 0}
            
            available_funds, account_balance = await self.get_user_portfolio(portfolio_id)
            if not (available_funds and account_balance):
                return {"Message": "Portfolio not found", "status": 0}
            
            price = trade_data[0].get('price', 0)
            symbol = trade_data[0].get('symbol')
            
            if status == 'Pending':
                bond_quantity = trade_data[0].get('quantity', 0)
                remaining_quantity = 0
            else:
                execution = trade_data[0].get('executions', [{}])[-1]
                remaining_quantity = execution.get('remaining_quantity', 0)
                bond_quantity = trade_data[0].get('quantity', 0) - remaining_quantity
                inc_leverage_trades, leverage_ratio = await oms.calculate_trade_calculation(
                    leverage_data=None, entry_price=price, quantity=bond_quantity
                )
            
            await self.update_portfolio(
                trade_data, account_balance, available_funds, status, volume=bond_quantity,
                updated_price=price, inc_leverage_trades=inc_leverage_trades if status == 'Partial Filled' else None
            )
            
            MatchingEngineCancelModifyTrade(trade_id=new_modification.trade_id).CancelTrade()
            return self.confirmation_message(status, symbol, price, bond_quantity, remaining_quantity)
        
        except Exception as e:
            logger_object['error'].log(f"CancelTrade-operation_perform: {e}")
    
    async def get_user_portfolio(self, portfolio_id):
        try:
            portfolio_data = await mo.fetch_user_portfolios(
                collection="portfolio", portfolio_id=str(portfolio_id), single_portfolio=True
            )
            return (
                portfolio_data[0].get('availableFund', 0),
                portfolio_data[0].get('accountBalance', 0)
            ) if portfolio_data else (None, None)
        except Exception as e:
            logger_object['error'].log(f"CancelTrade-get_user_portfolio: {e}")
    
    async def update_portfolio(self, trade_data, account_balance, available_funds, status, volume=None, updated_price=None, inc_leverage_trades=None):
        try:
            trade_id = trade_data[0].get('trade_id', "abcd")
            portfolio_id = trade_data[0].get('portfolio_id', 0)
            side = trade_data[0].get('side', 'flat')
            
            trade_ledger = await mo.select(collection_name="trade_ledger", filter_criteria={'trade_id': str(trade_id)})
            if trade_ledger:
                balance_adjustment = trade_ledger[0].get('update_balance', 0)
                available_funds += balance_adjustment if side == 'buy' else -balance_adjustment
            
            if status == 'Pending':
                await mo.return_and_delete_document(
                    collection_name="trade_ledger", filter_criteria={"trade_id": str(trade_id)}, mongodb_client="Traderverse-Authentication"
                )
                await mo.update(
                    collection="portfolio", filer={"_id": str(portfolio_id)},
                    operation={"accountBalance": account_balance, "availableFund": available_funds}
                )
                return
            
            symbol = trade_data[0].get('symbol', "Unknown")
            # stockId = trade_data[0].get('stockId', "AZABCDERTR")
            
            # 1. Fetch latest close price
            c_price = get_latest_stock_price(symbol)
            c_price = json.loads(c_price)
            
            if c_price is None:
                return {'result': 'Failed to fetch live price data', 'status': 0}
            
            close_price = c_price.get("price",0)

            user_id = trade_data[0].get('user_id', 0)
            
            await PortfolioManager(user_id=user_id, portfolio_id=portfolio_id).update_balance_and_fund(
                side, account_balance, available_funds, inc_leverage_trades, current_quantity=volume,
                close_price=close_price, entry_price=updated_price,
                portfolio_id=portfolio_id, trade_id=trade_id
            )
        except Exception as e:
            logger_object['error'].log(f"CancelTrade-update_portfolio: {e}")
    
    def confirmation_message(self, status, symbol, price, volume, remaining_quantity):
        message = (f"The trade for {symbol} with a limit price of ${price} is being canceled. "
                   f"The available quantity of {volume} will be traded, while the remaining quantity of {remaining_quantity} will be canceled.")
        return {"success": message, "status": 1} if status == 'Partial Filled' else {
            "success": f"Your pending trade for {symbol} has been successfully cancelled", "status": 1
        }


import datetime
from RefactorCode.HistoricalTrade import HistoricalTradeExecutionManager

class HisToricalTradeExecution():
    def __init__(self, high, low, close, volume):
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    async def show_trade_direction(self, user_id, symbol, quantity, side, portfolio_id, price=None, trade_date=None, order_type="market", tif_type="GTC", stop_loss=None, take_profit=None):
        try:
            #get latest stock price
            close_price = self.close

            new_order = Order(
                user_id=user_id,
                symbol=symbol,
                quantity=quantity,
                price=price,
                side=side,
                portfolio_id=portfolio_id,
                order_type=order_type,  # Can be market, stop, stop-limit
                tif_type=tif_type,  # Can be GTC, IOC, FOK, GTD,
                stop_loss=stop_loss,
                take_profit=take_profit

            )

            # validate order parameters
            try:
                order_validator = OrderValidator(
                    user_id=new_order.user_id,
                    symbol=new_order.symbol,
                    quantity=new_order.quantity,
                    price=new_order.price,
                    side=new_order.side,
                    portfolio_id=new_order.portfolio_id,
                    order_type=new_order.order_type,
                    tif_type=new_order.tif_type,
                    asset_name=new_order.asset_name,
                    stop_loss=new_order.stop_loss,
                    take_profit=new_order.take_profit,
                    close_price = close_price

                )
                order_validator.validate_order()
            except ValueError as e:
                return {'result': str(e), 'status': 0}

            if trade_date is None:
                trade_date = datetime.datetime.utcnow()
            
            #if price is none not given as for market get latest close price
            if new_order.price is None:
                new_order.price = self.close

            # trade calculation on the behalf of leverages
            leverage_data = await mo.select(collection_name="discord_leverages", filter_criteria={'portfolios_id': str(new_order.portfolio_id), 'asset_name': new_order.asset_name})
            inc_leverage_trades, leverage_ratio = await oms.calculate_trade_calculation(leverage_data, entry_price=new_order.price, quantity=new_order.quantity)
            
            # fetched user portfolios
            portfolio_data = await mo.fetch_user_portfolios(collection="portfolio", portfolio_id=str(new_order.portfolio_id), single_portfolio=True)
            account_balance = portfolio_data[0].get('accountBalance', 0)
            available_funds = float(portfolio_data[0].get('availableFund', 0))
            
            #calculate liquidation price
            liquidation_price = oms.calculate_liquidation_price(entry_price = float(new_order.price), leverage_ratio=leverage_ratio)
            
            #if fund is not enough reject trade
            if inc_leverage_trades['total_deductions'] >= available_funds:
                    return {'result': self._reject_trade(), 'status': 0}
            # handle balance deduction and addition
            user_portfolio = PortfolioManager(user_id=new_order.user_id, portfolio_id=new_order.portfolio_id)
            await user_portfolio.update_balance_and_fund(side, account_balance, available_funds, inc_leverage_trades, current_quantity=new_order.quantity, close_price = self.close, entry_price=new_order.price, portfolio_id=new_order.portfolio_id, trade_id=new_order.trade_id)

            hte = HistoricalTradeExecutionManager()
            return await hte._process_buy_or_sell(low_price=self.low, high_price=self.high, entry_price=new_order.price, trade_id=new_order.trade_id, full_user_name=None, trade_date=trade_date, quantity=new_order.quantity, side=new_order.side, high_volume=self.volume, symbol=new_order.symbol, portfolio_id=new_order.portfolio_id, user_id=new_order.user_id, order_type=new_order.order_type, stop_loss=new_order.stop_loss, take_profit=new_order.take_profit, liquidation_price=liquidation_price)
        
        except Exception as e:
            logger_object['error'].log(f"Historical_show_trade_direction: {e}")  
    
    def _reject_trade(self):
        return("Your trade has been rejected due to insufficient balance")
        


        



