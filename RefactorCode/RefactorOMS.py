import os 
import json
import uuid
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone
from bson import ObjectId 

from Orchestration.MongoDBOperation import MongodbOperation
from __init__ import logger_object

from dotenv import load_dotenv
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

class SymbolLoader:
    _symbols = None 

    @classmethod
    def get_symbols(cls):
        """Load stock symbols only once (lazy-loading)."""
        if cls._symbols is None:
            cwd = os.getcwd()
            stock_symbol_path = os.path.join(cwd, "RefactorCode", "stock_symbols.json")
            try:
                with open(stock_symbol_path) as f:
                    data = json.load(f)
                cls._symbols = {entry["symbol"] for entry in data}  # Store in memory
            except FileNotFoundError:
                print(f"Error: Stock symbols file not found at {stock_symbol_path}.")
                cls._symbols = set() 
        return cls._symbols

mo = MongodbOperation()

# Order Class
class Order:
    def __init__(self, user_id, portfolio_id, symbol, stockId, createdBy, side=None, price = None, quantity=None, order_type="limit", tif_type="GTC", asset_name='stocks', take_profit=None, stop_loss=None, expiry_date=None, trigger_price=None, trail_offset=None, direction=None):
        self.trade_id = str(uuid.uuid4())
        self.user_id = user_id
        self.portfolio_id = portfolio_id
        self.symbol = symbol
        self.quantity = quantity
        self.price = price
        self.order_type = order_type  # Limit, Market, Stop, Stop-Limit
        self.asset_name = asset_name.lower()
        self.tif_type = tif_type  # GTC, IOC, FOK, GTD
        self.expiry_date = expiry_date
        self.trigger_price = trigger_price
        self.trail_offset = trail_offset
        self.trade_date = datetime.now(timezone.utc)
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.direction = direction
        self.stockId = stockId
        self.createdBy = createdBy
        if order_type not in ['stoploss','takeprofit']:
           self.side = side.lower()


# Order Management System (OMS)
class OrderManagementSystem:
    def __init__(self):
        pass
    
    async def calculate_trade_calculation(self, leverage_data, entry_price, quantity):
        try:
            if leverage_data:
                leverage_ratio = leverage_data[0]['leverage_ratio']
                numerator = float(leverage_ratio.split(':')[0])
                commission = leverage_data[0]['commission']
                if commission == 'yes':
                    commission_rate=leverage_data[0]['commission_rate']
                    commission_type=leverage_data[0]['commission_type']
                else:
                    commission_type = 'percent'
                    commission_rate =  0.001
                
            else:
                numerator = 1.0
                commission_type = 'percent' 
                commission_rate =  0.001
                leverage_ratio = '1:1'

            broker_fee = 0.11

            inc_leverage_trades = await self.calculate_trade_summary(price=float(entry_price), quantity=float(quantity), leverage_ratio=numerator, broker_fee=broker_fee, commission_fee=commission_rate, commission_type=commission_type)
            logger_object['info'].log(f"Trade calculation data for entry price: {entry_price}, quantity: {quantity}, and leverage ratio: {leverage_ratio} is {inc_leverage_trades}.")  

            return inc_leverage_trades, leverage_ratio
        
        except Exception as e:
            logger_object['error'].log(f"OrderManagementSystem-calculate_trade_calculation: {e}")    


    async def calculate_trade_summary(
        self,
        price: float,
        quantity: float,
        leverage_ratio: float = 1.0,  # e.g., 4 means 4:1 leverage
        commission_type: str = 'flat_fee',
        commission_fee: float = 0.0,
        broker_fee: float = 0.0
            ):
        try:
            # Step 1: Calculate Total Trade Value
            total_trade_value = price * quantity

            # Step 2: Calculate margin requirement from leverage ratio
            # e.g., leverage_ratio=4 => margin_requirement = total_trade_value / 4
            margin_requirement = total_trade_value / leverage_ratio

            # Liquidation price
            liquidation_price = self.calculate_liquidation_price(price, leverage_ratio, max_loss_pct=1.0)

            # Step 3: Calculate borrowed money (amount borrowed from the broker)
            borrowed_money = total_trade_value - margin_requirement

            # Step 4: Commission Fee Calculation
            if commission_type == 'flat_fee':
                # e.g., $5 per trade
                commission_deduction = commission_fee
            elif commission_type == 'share/coin_based_fee':
                # e.g., $0.01/share
                commission_deduction = commission_fee * quantity
            elif commission_type == 'percentile_based_fee':
                # e.g., 0.001 => 0.1% of total trade value
                commission_deduction = total_trade_value * commission_fee
            else:
                commission_deduction = 0.0

            # Step 5: Broker Fee Calculation
            annual_broker_fee_deduction = (borrowed_money * broker_fee) / 365

            # Step 6: Calculate total deductions (excluding ongoing daily interest compounding)
            total_deductions = margin_requirement + commission_deduction + annual_broker_fee_deduction

            # Return results as a dictionary for better structure
            return {
                "total_trade_value": total_trade_value,
                "borrowed_money": borrowed_money,
                "margin_requirement": margin_requirement,
                "commission_deduction": commission_deduction,
                "annual_broker_fee_deduction": annual_broker_fee_deduction,
                "total_deductions": total_deductions,
                "liquidation_price": liquidation_price
            }
        
        except Exception as e:
            print(f"OrderManagementSystem-calculate_trade_summary: {e}")    
            return {
                "total_trade_value": 0,
                "borrowed_money": 0,
                "margin_requirement": 0,
                "commission_deduction": 0,
                "annual_broker_fee_deduction": 0,
                "total_deductions": 0,
                "liquidation_price": 0
            }
    
    def calculate_liquidation_price(self, entry_price, leverage_ratio="1:1", max_loss_pct=1.0, side=None):
        """
        Calculates the liquidation price for a short position when leverage is used.
        Parameters:
        entry_price (float): The average fill price of the short position.
        quantity (float): The number of shares shorted.
        leverage (float): The leverage used (e.g., 5 for 5x leverage).
        max_loss_pct (float): The maximum loss allowed as a fraction of initial capital (default is 1.0, i.e. 100%).
        Returns:
        float: The price at which the short position will be liquidated.
        Calculation:
        initial_capital = (entry_price * quantity) / leverage
        Maximum allowed loss = initial_capital * max_loss_pct
        (liquidation_price - entry_price) * quantity = initial_capital * max_loss_pct
        => liquidation_price = entry_price + (entry_price * max_loss_pct / leverage)
        => liquidation_price = entry_price * (1 + max_loss_pct / leverage)
        """
        try:
            if isinstance(leverage_ratio, str):
                leverage = float(leverage_ratio.split(':')[0]) 
            else:
                leverage = float(leverage_ratio)
        
            liquidation_price = entry_price * (1 + max_loss_pct / leverage)
            # logger_object['info'].log(f"Liquidation price for entry price: {entry_price} and leverage ratio: {leverage} is {liquidation_price}.")
            print(f"Liquidation price for entry price: {entry_price} and leverage ratio: {leverage} is {liquidation_price}.")  
              

            return liquidation_price
        except Exception as e:
            # logger_object['error'].log(f"Error in calculating short liquidation price: {e}")  
            print(f"Error in calculating short liquidation price: {e}")  
            return None

# Order Validator
class OrderValidator():
    def __init__(self, user_id, portfolio_id, symbol, price, order_type, asset_name, tif_type, close_price, side=None, direction=None, quantity=None, expiry_date=None, trigger_price=None, trail_offset=None, take_profit=None, stop_loss=None):
        self.user_id = user_id
        self.portfolio_id = portfolio_id
        self.symbol = symbol
        self.quantity = quantity
        self.price = price
        self.side = side
        self.order_type = order_type.lower()
        self.asset_name = asset_name.lower()
        self.tif_type = tif_type.upper()
        self.expiry_date = expiry_date
        self.trigger_price = trigger_price
        self.trail_offset = trail_offset
        self.trade_date = datetime.now(timezone.utc)
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.close_price = close_price,
        self.direction = direction

    def validate_order(self):
        """Validates order parameters before execution."""

        valid_order_types = {"limit", "market", "stop_order", "stop_limit", "trailing_stop", "takeprofit", "stoploss"}
        if self.order_type not in valid_order_types:
            raise ValueError({"Message":f"Error: {self.order_type} is not a valid order type.", "status":0})

        valid_asset_types = {"stocks", "crypto", "forex"}
        if self.asset_name not in valid_asset_types:
            raise ValueError({"Message":f"Error: {self.asset_name} is not a valid asset type.", "status":0})

        valid_tif_types = {"GTC", "GTD", "IOC", "FOK", "DAY", "OPG", "CLS", "MOC", "LOC"}
        if self.tif_type not in valid_tif_types:
            raise ValueError({"Message":f"Error: {self.tif_type} is not a valid TIF type.", "status":0})

        # if self.symbol not in symbols:
        if self.symbol not in SymbolLoader.get_symbols():
            raise ValueError({"Message":f"Error: {self.symbol} is not a valid stock symbol.", "status":0})

        if self.order_type not in ['stoploss', 'takeprofit']:
            if not isinstance(self.quantity, (int, float)) or self.quantity <= 0:
                raise ValueError({"Message":"Error: Quantity must be a positive number.", "status":0})

        if self.price is not None:
            if not isinstance(self.price, (int, float)) or self.price <= 0:
                raise ValueError({"Message":"Error: Price must be a positive number.", "status":0})
        
        if self.price is None:
            self.price = self.close_price
        if self.direction is None:
            if self.side == 'buy':
                self.direction = "long"
            elif self.side == 'sell':
                self.direction = 'short'

        # Valid directions
        valid_directions = {'long', 'short'}
        if hasattr(self, 'direction') and self.direction.lower() not in valid_directions:
            raise ValueError({"Message": f"Invalid direction: {self.direction}", "status": 0})

        # Validate Take Profit
        if self.take_profit is not None:
            if not isinstance(self.take_profit, (int, float)):
                raise ValueError({"Message": "Error: Take profit must be a numeric value.", "status": 0})

            if self.side == 'buy':
                if self.direction == 'long' and self.take_profit <= self.price:
                    raise ValueError({
                        "Message": f"Error: For BUY-LONG, take profit must be greater than entry price: {self.price}.",
                        "status": 0
                    })
                elif self.direction == 'short' and self.take_profit >= self.price:
                    raise ValueError({
                        "Message": f"Error: For BUY-SHORT, take profit must be less than entry price: {self.price}.",
                        "status": 0
                    })
            elif self.side == 'sell':
                if self.direction == 'long' and self.take_profit <= self.price:
                    raise ValueError({
                        "Message": f"Error: For SELL-LONG, take profit must be greater than entry price: {self.price}.",
                        "status": 0
                    })
                elif self.direction == 'short' and self.take_profit >= self.price:
                    raise ValueError({
                        "Message": f"Error: For SELL-SHORT, take profit must be less than entry price: {self.price}.",
                        "status": 0
                    })
            else:
                raise ValueError({"Message": f"Invalid side: {self.side}", "status": 0})

        # Validate Stop Loss
        if self.stop_loss is not None:
            if not isinstance(self.stop_loss, (int, float)):
                raise ValueError({"Message": "Error: Stop loss must be a numeric value.", "status": 0})

            if self.side == 'buy':
                if self.direction == 'long' and self.stop_loss >= self.price:
                    raise ValueError({
                        "Message": f"Error: For BUY-LONG, stop loss must be less than entry price: {self.price}.",
                        "status": 0
                    })
                elif self.direction == 'short' and self.stop_loss <= self.price:
                    raise ValueError({
                        "Message": f"Error: For BUY-SHORT, stop loss must be greater than entry price: {self.price}.",
                        "status": 0
                    })
            elif self.side == 'sell':
                if self.direction == 'long' and self.stop_loss >= self.price:
                    raise ValueError({
                        "Message": f"Error: For SELL-LONG, stop loss must be less than entry price: {self.price}.",
                        "status": 0
                    })
                elif self.direction == 'short' and self.stop_loss <= self.price:
                    raise ValueError({
                        "Message": f"Error: For SELL-SHORT, stop loss must be greater than entry price: {self.price}.",
                        "status": 0
                    })
            else:
                raise ValueError({"Message": f"Invalid side: {self.side}", "status": 0})



        if self.side not in {"buy", "sell"}:
            raise ValueError({"Message":"Error: Side must be 'buy' or 'sell'.", "status":0})

        if self.order_type in {"stoporder", "stoplimit"} and self.trigger_price is not None:
            if not isinstance(self.trigger_price, (int, float)) or self.trigger_price <= 0:
                raise ValueError({"Message":"Error: Trigger price must be a positive number.", "status":0})

        return True

    def takeprofit_stoploss_validator(self, direction, price, holding_price, holding_quantity):
        holding_quantity = abs(holding_quantity)

        if self.order_type in {"stoploss", "takeprofit"}:
            if direction is None:  
                raise ValueError({"Message":"Error: Direction must not be None when using stoploss or takeprofit.", "status":0})
            
            if not isinstance(direction, str) or direction.lower() not in {"short", "long"}:
                raise ValueError({"Message":"Error: Direction must be either 'short' or 'long'.", "status":0})

            if price is None:
                raise ValueError({"Message":"Error: Price must not be None when using stoploss or takeprofit.", "status":0})

            # ✅ LONG (BUY) POSITION -> Exit by SELLING
            if direction == "long":
                if self.quantity > holding_quantity:
                    raise ValueError({"Message":f"Error: Selling quantity {self.quantity} exceeds holding quantity {holding_quantity} for a long position.", "status":0})

                if self.order_type == "takeprofit":
                    if price <= holding_price:  # TP should be GREATER than holding price
                        raise ValueError({"Message":f"Error: Take profit price must be greater than {holding_price} for a long position.", "status":0})

                if self.order_type == "stoploss":
                    if price >= holding_price:  # SL should be LESSER than holding price
                        raise ValueError({"Message":f"Error: Stop loss price must be lesser than {holding_price} for a long position.", "status":0})
                
                if self.side != "sell":
                    raise ValueError({"Message":"Error: Side must be 'sell' for a long position.", "status":0})

            # ✅ SHORT (SELL) POSITION -> Exit by BUYING
            if direction == "short":
                if self.quantity > holding_quantity:
                    raise ValueError({"Message":f"Error: Buying quantity {self.quantity} must be at least the short holding quantity {holding_quantity}.", "status":0})
        
                if self.order_type == "takeprofit":
                    if price >= holding_price:  # TP should be LESSER than holding price
                        raise ValueError({"Message":f"Error: Take profit price must be lesser than {holding_price} for a short position.", "status":0})

                if self.order_type == "stoploss":
                    if price <= holding_price:  # SL should be GREATER than holding price
                        raise ValueError({"Message":f"Error: Stop loss price must be greater than {holding_price} for a short position.", "status":0})
                
                if self.side != "buy":
                    raise ValueError({"Message":"Error: Side must be 'buy' for a short position.", "status":0})


#  Portfolio Manager
class PortfolioManager:
    def __init__(self, user_id, portfolio_id): 
        self.user_id = user_id
        self.portfolio_id = portfolio_id
    
    def to_money(self, value):
        return float(Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    async def update_balance_and_fund(
        self,
        side,
        account_balance,
        available_funds,
        inc_leverage_trades,
        current_quantity,
        close_price,
        entry_price,
        portfolio_id,
        trade_id=None,
        symbol=None
    ):
        try:
            if trade_id is None:
                trade_id = self.trade_id

            short_sell_fee = 5
            side = side.lower()
            before_account_balance, before_available_funds = account_balance, available_funds

            # Extract trade details
            trade_value = self.to_money(inc_leverage_trades.get('total_trade_value', 0))
            margin_requirement = self.to_money(inc_leverage_trades.get('margin_requirement', 0))
            commission_fee = self.to_money(inc_leverage_trades.get('commission_deduction', 0))
            broker_fee = self.to_money(inc_leverage_trades.get('annual_broker_fee_deduction', 0))

            existing_quantity = 0
            if symbol:
                existing_position = await mo.select(
                    collection_name="AverageTransactions",
                    filter_criteria={'portfolio_id': str(portfolio_id), 'symbol': symbol}
                )
                if existing_position:
                    existing_quantity = existing_position[0].get('quantity', 0)

            realized_pnl = 0

            # === BUY SIDE ===
            if side == "buy":
                if existing_quantity >= 0:
                    # Open long or scale-in
                    available_funds -= margin_requirement + commission_fee + broker_fee
                    account_balance -= commission_fee + broker_fee
                    print(f"Action: New Long or Scale-In | Symbol: {symbol}")
                else:
                    abs_existing = abs(existing_quantity)
                    if current_quantity <= abs_existing:
                        # Buy to cover (fully or partially)
                        realized_pnl = self.to_money((entry_price - close_price) * current_quantity)
                        available_funds += realized_pnl + margin_requirement - commission_fee - broker_fee - short_sell_fee
                        account_balance += realized_pnl - commission_fee - broker_fee - short_sell_fee
                        print(f"Action: Buy to Cover Short | Realized PnL: {realized_pnl} | Symbol: {symbol}")
                    else:
                        # Flip short to long
                        cover_qty = abs_existing
                        new_long_qty = current_quantity - abs_existing
                        realized_pnl = self.to_money((entry_price - close_price) * cover_qty)

                        released_margin = cover_qty * entry_price
                        long_value = new_long_qty * entry_price
                        long_margin = long_value

                        # Add back released margin and PnL
                        available_funds += released_margin + realized_pnl
                        # Deduct margin for long side
                        available_funds -= long_margin + commission_fee + broker_fee
                        account_balance -= commission_fee + broker_fee

                        print(f"Action: Flip Short to Long | Realized PnL: {realized_pnl} | Symbol: {symbol} | New Long Qty: {new_long_qty}")

            # === SELL SIDE ===
            elif side == "sell":
                if existing_quantity <= 0:
                    # Open short or scale-in
                    available_funds -= margin_requirement + commission_fee + broker_fee + short_sell_fee
                    account_balance -= commission_fee + broker_fee + short_sell_fee
                    print(f"Action: New Short or Scale-In | Symbol: {symbol}")
                else:
                    if current_quantity <= existing_quantity:
                        # Sell to close long
                        realized_pnl = self.to_money((close_price - entry_price) * current_quantity)
                        available_funds += realized_pnl + margin_requirement - commission_fee - broker_fee
                        account_balance += realized_pnl - commission_fee - broker_fee
                        print(f"Action: Sell to Close Long | Realized PnL: {realized_pnl} | Symbol: {symbol}")
                    else:
                        # Flip long to short
                        sell_qty = existing_quantity
                        new_short_qty = current_quantity - existing_quantity
                        realized_pnl = self.to_money((close_price - entry_price) * sell_qty)

                        released_margin = sell_qty * entry_price
                        short_value = new_short_qty * entry_price
                        short_margin = short_value

                        # Add back released margin and PnL
                        available_funds += released_margin + realized_pnl
                        # Deduct margin for new short
                        available_funds -= short_margin + commission_fee + broker_fee + short_sell_fee
                        account_balance -= commission_fee + broker_fee + short_sell_fee

                        print(f"Action: Flip Long to Short | Realized PnL: {realized_pnl} | Symbol: {symbol} | New Short Qty: {new_short_qty}")

            # Final rounding
            account_balance = self.to_money(account_balance)
            available_funds = self.to_money(available_funds)

            print(f"Before: Account balance: {before_account_balance}, Available Funds: {before_available_funds}")
            print(f"After:  Account balance: {account_balance}, Available Funds: {available_funds}")

            # Record trade ledger update
            await mo.update_and_missing_key(
                collection="trade_ledger",
                filer={"trade_id": str(trade_id)},
                operation={'side': side, 'update_balance': before_available_funds - available_funds}
            )

            # Update portfolio balances
            await mo.update(
                collection="portfolio",
                filer={"_id": str(portfolio_id)},
                operation={"accountBalance": account_balance, "availableFund": available_funds}
            )

        except Exception as e:
            print(f"Error: update_balance_and_fund: {e}")
    
    async def porfolio_handling_for_tp_and_sl(self, available_funds, account_balance, quantity, price, order_type, direction, holding_price, holding_quantity):

        # **Handle Take Profit / Stop Loss Execution**
        realized_pnl = 0
        total_trade_value = quantity * price

        if order_type == "takeprofit":
            if direction == "long":
                # ✅ Take Profit (Sell at TP price - Realizing Profit)
                realized_pnl = (price - holding_price) * quantity
                available_funds += total_trade_value  
                holding_quantity -= quantity  

            else:  # Short TP Case (BUY to cover short at profit)
                realized_pnl = (holding_price - price) * quantity
                available_funds -= total_trade_value  
                holding_quantity -= quantity  

        elif order_type == "stoploss":
            if direction == "long":
                # ✅ Stop Loss (Sell at SL price - Booking Loss)
                realized_pnl = (price - holding_price) * quantity
                available_funds += total_trade_value 
                holding_quantity -= quantity  # Reduce long holdings

            else:  # Short SL Case (BUY to cover short at a loss)
                realized_pnl = (holding_price - price) * quantity
                available_funds -= total_trade_value 
                holding_quantity -= quantity  

        # ✅ Account Balance Adjustment
        if holding_quantity == 0:
            account_balance += realized_pnl 

        # ✅ Update Portfolio Balance in MongoDB
        await mo.update_and_missing_key(
            collection="portfolio",
            filer={"_id": str(self.portfolio_id)},
            operation={"accountBalance": account_balance, "availableFund": available_funds}
        )


class ModifyInput:
    def __init__(self, trade_id, update_price):
        self.trade_id = trade_id
        self.update_price = update_price

class CancelInput:
    def __init__(self, trade_id):
        self.trade_id = trade_id

class ValidateModifyCancelInput:
    def __init__(self, trade_id, update_price=None):
        if not trade_id:
            raise ValueError({"Message":"Error: Trade ID must not be None or empty.", "status":0})
        
        self.trade_id = trade_id
        self.update_price = update_price

    def edit_trades(self, trade_data):
        """Validate trade edit request."""
        if self.update_price is None or not isinstance(self.update_price, (int, float)) or self.update_price <= 0:
            raise ValueError({"Message": "Error: Update price must be a positive number.", "status":0})

        if trade_data is None:
            raise ValueError({"Message":"Error: No Data found in this TradeID", "status":0})
        
        if trade_data[0].get('status', None) != 'Pending':
            raise ValueError({"Message": "Error: Only pending trade can be modified.", "status": 0})
        
    def cancel_trades(self, trade_data):
        """Validate trade cancellation request."""
        if trade_data is None:
            raise ValueError({"Message":"Error: No Data found in this TradeID", "status":0})


class TradeAndTransactionRecord():
    def __init__(self, trade_id, symbol, side, order_type, quantity, portfolio_id, user_id, tif_type, createdBy, price, asset_type, stockId, stop_loss=None, take_profit=None, liquidation_price=None, username=None, direction=None):
        self.trade_id = trade_id
        self.username = username
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.quantity = quantity
        self.portfolio_id = portfolio_id
        self.user_id = user_id
        self.entry_date = datetime.now(timezone.utc)
        self.liquidation_price = liquidation_price
        self.direction = direction
        self.tif_type = tif_type
        self.createdBy = createdBy
        self.asset_type = asset_type
        self.stockId = stockId
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.price = price

    async def add_record_in_trade_and_transaction(self):
        try:

            order_place_document_trades = {
                "trade_id": self.trade_id,
                "username": self.username,
                'symbol': self.symbol.upper(),
                "side": self.side,
                'order_type': self.order_type,
                'remaining_quantity': self.quantity,
                "quantity": self.quantity,
                "entry_price": self.price,
                "price": None,
                'status': 'Pending',
                'portfolio_id': self.portfolio_id,
                'user_id': self.user_id,
                "entry_date": self.entry_date,
                "liquidation_price": self.liquidation_price,
                "direction": self.direction,
                "tif_type": self.tif_type.upper(),
                "createdBy": self.createdBy.capitalize(),
                "asset_type": self.asset_type.lower(),
                "stockId": self.stockId.upper(),
                "stop_loss": self.stop_loss, 
                "take_profit": self.take_profit,
                "created_at": datetime.utcnow()
            }

            await mo.insert(document=order_place_document_trades, collection=trade_table, mongodb_client="Traderverse-Authentication")
            logger_object["success"].log(f"Success: successfully add document in trades table with trade_id: {self.trade_id}")

            order_place_document_transaction = {
                "transaction_id": ObjectId(),  # Unique ID for each transaction
                "username": self.username,
                'symbol': self.symbol.upper(),
                "quantity": self.quantity,
                "entry_price": self.price,
                "price": None,
                "entry_date": self.entry_date,
                "trade_parameter": 'Pending',
                'order_type': self.order_type,
                "side": self.side,
                "trade_id": self.trade_id,
                'portfolio_id': self.portfolio_id,
                'user_id': self.user_id,
                "liquidation_price": self.liquidation_price,
                "tif_type": self.tif_type.upper(),
                "createdBy": self.createdBy.capitalize(),
                "asset_type": self.asset_type.lower(),
                "stockId": self.stockId.upper(),
                "stop_loss": self.stop_loss, 
                "take_profit": self.take_profit,
                "created_at": datetime.utcnow(),
            }

            await mo.insert(document=order_place_document_transaction, collection=transaction_table, mongodb_client="Traderverse-Authentication")
            logger_object["success"].log(f"Success: successfully add document in transaction table with trade_id: {self.trade_id}")
        
        except Exception as e:
            logger_object["error"].log(f"Error: add_record_in_trade_and_transaction: Face error {e}")



class TpAndSlRecord():
    def __init__(self, trade_id, symbol, side, order_type, portfolio_id, user_id, createdBy, price, asset_type, stockId, direction, username=None):
        self.trade_id = trade_id
        self.username = username
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.portfolio_id = portfolio_id
        self.user_id = user_id
        self.entry_date = datetime.now(timezone.utc)
        self.direction = direction
        self.createdBy = createdBy
        self.asset_type = asset_type
        self.stockId = stockId
        self.price = price

    async def add_record_in_tp_and_sl(self):
        try:
            order_place_document_trades = {
                "trade_id": self.trade_id,
                "username": self.username,
                'symbol': self.symbol.upper(),
                "side": self.side,
                'order_type': self.order_type,
                "entry_price": self.price,
                'status': 'Pending',
                'portfolio_id': self.portfolio_id,
                'user_id': self.user_id,
                "entry_date": self.entry_date,
                "direction": self.direction,
                "createdBy": self.createdBy.capitalize(),
                "asset_type": self.asset_type.lower(),
                "stockId": self.stockId.upper(),
                "created_at": datetime.utcnow()
            }

            await mo.insert(document=order_place_document_trades, collection=trade_table, mongodb_client="Traderverse-Authentication")
            logger_object["success"].log(f"Success: successfully add document in trades table with trade_id: {self.trade_id}")

            order_place_document_transaction = {
                "transaction_id": ObjectId(),  # Unique ID for each transaction
                "username": self.username,
                'symbol': self.symbol.upper(),
                "entry_price": self.price,
                "entry_date": self.entry_date,
                "trade_parameter": 'Pending',
                'order_type': self.order_type,
                "side": self.side,
                "trade_id": self.trade_id,
                'portfolio_id': self.portfolio_id,
                'user_id': self.user_id,
                "createdBy": self.createdBy.capitalize(),
                "asset_type": self.asset_type.lower(),
                "stockId": self.stockId.upper(),
                "created_at": datetime.utcnow(),
            }

            await mo.insert(document=order_place_document_transaction, collection=transaction_table, mongodb_client="Traderverse-Authentication")
            logger_object["success"].log(f"Success: successfully add document in transaction table with trade_id: {self.trade_id}")
        
        except Exception as e:
            logger_object["error"].log(f"Error: add_record_in_tp_and_sl: Face error {e}")





