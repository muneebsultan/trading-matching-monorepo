import os
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from Core.general import DatabaseManager
from Core.__init__ import logger_object

from dotenv import load_dotenv
load_dotenv()
trade_table = os.getenv("TRADE_TABLE")
transaction_table = os.getenv("TRANSACTION_TABLE")
avg_transaction_table = os.getenv("AVG_TRANSACTION_TABLE")

class HandlePortfolioBalanceAndFund():
    def __init__(self, portfolio_id, asset_name, price, quantity, side, symbol, trade_id, user_id, close_price):
        self.portfolio_id = portfolio_id
        self.asset_name = asset_name
        self.quantity = quantity
        self.price = price
        self.side = side
        self.symbol = symbol
        self.trade_id = trade_id
        self.user_id = user_id
        self.close_price = close_price
        self.mo = DatabaseManager()

    async def HandlePortfolioHub(self):
        try:
            if self.asset_name:
                try:
                # 2. Fetch leverage info
                    leverage_data = self.mo.select(
                        collection_name="discord_leverages",
                        filter_criteria={'portfolios_id': str(self.portfolio_id), 'asset_name': self.asset_name}
                    )
                except Exception as e:
                    logger_object['error'].log(f"HandlePortfolioBalanceAndFund-leverage_data: ⚠️ Error while fetching leverage: {e}")
                    leverage_data = None
            else:
                leverage_data = None


            inc_leverage_trades, leverage_ratio = await self.calculate_trade_calculation(
                leverage_data, entry_price=self.price, quantity=self.quantity
            )

            # 3. Get portfolio info
            portfolio_data = self.mo.fetch_user_portfolios(
                collection="portfolio",
                portfolio_id=str(self.portfolio_id),
                single_portfolio=True
            )

            if not portfolio_data:
                return {"result": "❌ Portfolio data not found", "status": 0}


            account_balance = portfolio_data[0].get('accountBalance', 0)
            available_funds = float(portfolio_data[0].get('availableFund', 0))

            # 4. Calculate liquidation price
            liquidation_price = self.calculate_liquidation_price(
                entry_price=float(self.price),
                leverage_ratio=leverage_ratio,
                side=self.side
            )

            # 5. Check for existing position (for flip)
            existing_position = self.mo.select(
                collection_name=avg_transaction_table,
                filter_criteria={'portfolio_id': str(self.portfolio_id), 'symbol': self.symbol}
            )

            current_quantity = 0
            if existing_position:
                current_quantity = existing_position[0].get('quantity', 0)

            if existing_position:
                if self.side == "sell" and current_quantity > 0:
                    if self.quantity > current_quantity:
                        short_qty = self.quantity - current_quantity
                        short_trade, _ = await self.calculate_trade_calculation(leverage_data, entry_price=self.price, quantity=short_qty)
                        released_data, _ = await self.calculate_trade_calculation(leverage_data, entry_price=self.price, quantity=current_quantity)
                        released_margin = released_data.get("margin_requirement", 0)
                        simulated_funds = available_funds + released_margin

                        if short_trade['total_deductions'] > simulated_funds:
                            return {
                                "error": f"❌ Not enough margin to short {short_qty} after closing long. Needed: {short_trade['total_deductions']}, Available: {simulated_funds}",
                                "status": 0
                            }

                elif self.side == "buy" and current_quantity < 0:
                    if self.quantity > abs(current_quantity):
                        long_qty = self.quantity - abs(current_quantity)
                        long_trade, _ = await self.calculate_trade_calculation(leverage_data, entry_price=self.price, quantity=long_qty)
                        released_data, _ = await self.calculate_trade_calculation(leverage_data, entry_price=self.price, quantity=abs(current_quantity))
                        released_margin = released_data.get("margin_requirement", 0)
                        simulated_funds = available_funds + released_margin

                        if long_trade['total_deductions'] > simulated_funds:
                            return {
                                "error": f"❌ Not enough margin to buy {long_qty} after covering short. Needed: {long_trade['total_deductions']}, Available: {simulated_funds}",
                                "status": 0
                            }
                else:
                    if inc_leverage_trades['total_deductions'] > available_funds:
                        return self._reject_trade()
            else:
                if inc_leverage_trades['total_deductions'] > available_funds:
                    return self._reject_trade()
                
            # 8. Deduct balance
            user_portfolio = PortfolioManager(user_id=self.user_id, portfolio_id=self.portfolio_id, trade_id=self.trade_id)
            user_portfolio.update_balance_and_fund(
                self.side,
                account_balance,
                available_funds,
                inc_leverage_trades,
                current_quantity=self.quantity,
                close_price=self.close_price,
                entry_price=self.price,
                portfolio_id=self.portfolio_id,
                trade_id=self.trade_id,
                symbol=self.symbol
            )
            logger_object['success'].log(f"HandlePortfolioBalanceAndFund-HandlePortfolioHub: Successfully handle balance and fund for trade_id {self.trade_id}, and portfolio_id: {self.portfolio_id}")

            logger_object['success'].log('"result": "✅ Trade processed successfully", "status": 1')
            return {"result": "✅ Trade processed successfully", "status": 1}
        
        except Exception as e:
            logger_object['error'].log(f"HandlePortfolioBalanceAndFund-HandlePortfolioHub: ⚠️ Error while handling balance: {e}")
    
    def _reject_trade(self):
        return {
            "result": "❌ Your trade has been rejected due to insufficient balance.",
            "status": 0
        }

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
            liquidation_price = self.calculate_liquidation_price(price, leverage_ratio, max_loss_pct=1.0, side=self.side)

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
            logger_object['error'].log(f"OrderManagementSystem-calculate_trade_summary: {e}")    
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

            if side == "buy":  # long
                liquidation_price = entry_price * (1 - max_loss_pct / leverage)
            elif side == "sell":  # short
                liquidation_price = entry_price * (1 + max_loss_pct / leverage)
            else:
                liquidation_price = None  # invalid use
            
            logger_object['info'].log(f"Liquidation price for entry price: {entry_price} and leverage ratio: {leverage} is {liquidation_price}.")

            return liquidation_price
        except Exception as e:
            logger_object['error'].log(f"Error in calculating short liquidation price: {e}")  
            return None

class PortfolioManager:
    def __init__(self, user_id, portfolio_id, trade_id): 
        self.user_id = user_id
        self.portfolio_id = portfolio_id
        self.trade_id = trade_id
        self.mo = DatabaseManager()
    
    def to_money(self, value):
        return float(Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    
    def update_balance_and_fund(
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

            # Extract trade fees
            commission_fee = self.to_money(inc_leverage_trades.get('commission_deduction', 0))
            broker_fee = self.to_money(inc_leverage_trades.get('annual_broker_fee_deduction', 0))
            total_fees = commission_fee + broker_fee

            existing_quantity = 0
            avg_open_price = 0.0
            if symbol:
                existing_position = self.mo.select(
                    collection_name=avg_transaction_table,
                    filter_criteria={'portfolio_id': str(portfolio_id), 'symbol': symbol}
                )
                if existing_position:
                    existing_quantity = existing_position[0].get('quantity', 0)
                    avg_open_price = float(existing_position[0].get('average_price', entry_price))  # fallback

            # === BUY SIDE ===
            if side == "buy":
                if existing_quantity >= 0:
                    # Open new long or scale-in
                    margin_to_freeze = self.to_money(current_quantity * entry_price)
                    available_funds -= margin_to_freeze + total_fees
                    account_balance -= total_fees
                    logger_object['info'].log(f"Action: New Long or Scale-In | Symbol: {symbol}")
                else:
                    abs_existing = abs(existing_quantity)
                    if current_quantity <= abs_existing:
                        # Buy to cover (fully or partially)
                        released_margin = self.to_money(current_quantity * avg_open_price)
                        trade_cost = self.to_money(current_quantity * close_price)

                        available_funds += released_margin
                        available_funds -= trade_cost
                        available_funds -= total_fees + short_sell_fee
                        account_balance -= total_fees + short_sell_fee

                        logger_object['info'].log(f"Action: Buy to Cover Short | Symbol: {symbol}")
                    else:
                        # Flip short to long
                        cover_qty = abs_existing
                        new_long_qty = current_quantity - abs_existing

                        released_margin = self.to_money(cover_qty * avg_open_price)
                        cover_cost = self.to_money(cover_qty * close_price)
                        long_margin = self.to_money(new_long_qty * entry_price)

                        # Unwind short
                        available_funds += released_margin
                        available_funds -= cover_cost

                        # Open long
                        available_funds -= long_margin + total_fees
                        account_balance -= total_fees

                        logger_object['info'].log(f"Action: Flip Short to Long | Symbol: {symbol} | New Long Qty: {new_long_qty}")

            # === SELL SIDE ===
            elif side == "sell":
                if existing_quantity <= 0:
                    # Open new short or scale-in
                    margin_to_freeze = self.to_money(current_quantity * entry_price)
                    available_funds -= margin_to_freeze + total_fees + short_sell_fee
                    account_balance -= total_fees + short_sell_fee
                    logger_object['info'].log(f"Action: New Short or Scale-In | Symbol: {symbol}")
                else:
                    if current_quantity <= existing_quantity:
                        # Sell to close long
                        released_margin = self.to_money(current_quantity * avg_open_price)
                        sale_proceeds = self.to_money(current_quantity * close_price)

                        available_funds += released_margin + sale_proceeds
                        available_funds -= total_fees
                        account_balance -= total_fees

                        logger_object['info'].log(f"Action: Sell to Close Long | Symbol: {symbol}")
                    else:
                        # Flip long to short
                        sell_qty = existing_quantity
                        new_short_qty = current_quantity - existing_quantity

                        released_margin = self.to_money(sell_qty * avg_open_price)
                        sale_proceeds = self.to_money(sell_qty * close_price)
                        short_margin = self.to_money(new_short_qty * entry_price)

                        # Close long
                        available_funds += released_margin + sale_proceeds

                        # Open short
                        available_funds -= short_margin + total_fees + short_sell_fee
                        account_balance -= total_fees + short_sell_fee

                        logger_object['info'].log(f"Action: Flip Long to Short | Symbol: {symbol} | New Short Qty: {new_short_qty}")

            # Final rounding
            account_balance = self.to_money(account_balance)
            available_funds = self.to_money(available_funds)

            logger_object['info'].log(f"Before: Account balance: {before_account_balance}, Available Funds: {before_available_funds}")
            logger_object['info'].log(f"After:  Account balance: {account_balance}, Available Funds: {available_funds}")

            # ✅ Single atomic Mongo updates (recommend MongoDB transaction if available)
            self.mo.update_and_missing_key(
                collection="trade_ledger",
                filer={"trade_id": str(trade_id)},
                operation={'side': side, 'update_balance': before_available_funds - available_funds}
            )

            self.mo.update(
                collection="portfolio",
                filer={"_id": str(portfolio_id)},
                operation={
                    "accountBalance": account_balance,
                    "availableFund": available_funds,
                    "lastModified": datetime.utcnow()
                }
            )

        except Exception as e:
            logger_object['error'].log(f"❌ update_balance_and_fund: {e}")
