import asyncio
import json
import os
from datetime import datetime, timezone
from dateutil import parser
import threading
import pytz

from Core.__init__ import logger_object
from nats_pipeline.main import NATSDataCollector


class NATSPriceService:
    def __init__(self):
        self.collector = NATSDataCollector()
        self._last_timestamp = {}
        self._start_collector_background()

    def _start_collector_background(self):
        """Start NATS collector in a background thread."""
        thread = threading.Thread(target=self._run_collector_loop, daemon=True)
        thread.start()

    def _run_collector_loop(self):
        """Run an event loop for the NATS collector."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._collector_loop())

    async def _collector_loop(self):
        """Connect and continuously listen to NATS."""
        connected = await self.collector.connect_to_nats()
        if connected:
            await self.collector.subscribe_to_subjects()
            logger_object['success'].log("✅ Connected and subscribed to NATS")

            # Keep the loop alive
            while True:
                await asyncio.sleep(10)
        else:
            logger_object['error'].log("❌ Failed to connect to NATS")

    def fetch_quote(self, symbol):
        """
        Fetch the latest quote for a symbol from NATS.
        Returns only essential OHLC, symbol and time data.
        """
        symbol = symbol.upper()
        try:
            ohlcv = self.collector.get_latest_ohlcv_by_symbol(symbol)
            logger_object['success'].log(f"OHLCV DATA:  {ohlcv} for symbol: {symbol}")
            
            if not ohlcv:
                logger_object['error'].log(f"No quote found for {symbol} in NATS")
                return None

            utc_now = datetime.now(pytz.UTC).isoformat()

            quote = {
                'symbol': symbol,
                'timestamp_UTC': ohlcv.get('ts', utc_now),
                'open': ohlcv['open'],
                'high': ohlcv['high'],
                'low': ohlcv['low'],
                'close': ohlcv['close'],
                'volume': ohlcv['volume'],
            }

            logger_object['success'].log(
                f"NATS data for {symbol}: OHLC={ohlcv['open']:.4f}/{ohlcv['high']:.4f}/{ohlcv['low']:.4f}/{ohlcv['close']:.4f}"
            )
            return quote
            # return {'symbol': 'MSFT', 'open': 464.34, 'high': 464.34, 'low': 464.34, 'close': 464.34, 'volume': 10, 'source': 'NASDAQ', 'timestamp_UTC': datetime.now(timezone.utc)}
        except Exception as e:
            logger_object['error'].log(f"fetch_quote: ⚠️ Error fetching from NATS: {e}")
            return None


    def get_latest_stock_price(self, symbol, user_trade_time=None):
        """
        Fetch the latest stock price for a symbol from NATS.
        Returns essential OHLCV, symbol, and time data.
        """
        try:
            if not hasattr(self, '_last_timestamp'):
                self._last_timestamp = {}

            symbol = symbol.upper()
            latest_quote = self.fetch_quote(symbol)

            if not latest_quote:
                logger_object['error'].log(f"get_latest_stock_price: ⚠️ No quote found for symbol: {symbol}")
                return None

            latest_time_val = latest_quote.get('timestamp_UTC')
            if not latest_time_val:
                logger_object['error'].log("Missing 'timestamp_UTC' in quote data")
                return None

            # Handle datetime or string timestamp_UTC
            if isinstance(latest_time_val, str):
                latest_time = parser.isoparse(latest_time_val)
            else:
                latest_time = latest_time_val  # assume datetime object

            # 🚫 Reject garbage timestamps
            if latest_time.year < 2000:
                logger_object['error'].log(f"Quote timestamp too old or invalid: {latest_time} for {symbol}")
                return None

            # Check against last seen timestamp
            last_seen_time_val = self._last_timestamp.get(symbol)
            if last_seen_time_val:
                if isinstance(last_seen_time_val, str):
                    last_seen_time = parser.isoparse(last_seen_time_val)
                else:
                    last_seen_time = last_seen_time_val

                if latest_time <= last_seen_time:
                    logger_object['info'].log(
                        f"[SKIP CHECK] latest_time={latest_time} | last_seen_time={last_seen_time} for symbol={symbol}"
                    )
                    return None

            # Compare with user trade time
            if user_trade_time:
                if isinstance(user_trade_time, str):
                    trade_time = parser.isoparse(user_trade_time)
                else:
                    trade_time = user_trade_time  # already a datetime
                
                if trade_time.tzinfo is None:
                    trade_time = trade_time.replace(tzinfo=timezone.utc)

                if latest_time.tzinfo is None:
                    latest_time = latest_time.replace(tzinfo=timezone.utc)

                if latest_time < trade_time:
                    logger_object['info'].log(f"get_latest_stock_price: Quote for {symbol} is older than trade time {trade_time} and user_trade_time {user_trade_time}")
                    # Optionally reset the tracker
                    self._last_timestamp.pop(symbol, None)
                    return None

            # ✅ Valid quote: update last seen timestamp
            self._last_timestamp[symbol] = latest_time

            logger_object['success'].log(f"get_latest_stock_price: {latest_quote} for symbol: {symbol}")

            # Return full OHLCV data if volume and source are present, else fallback to OHLC only
            result = [
                latest_time,
                symbol,
                float(latest_quote.get('close', 0)),
                float(latest_quote.get('volume', 0)),
                float(latest_quote.get('close', 0)),
                float(latest_quote.get('volume', 0)),
            ]

            return result

        except Exception as e:
            logger_object['error'].log(f"get_latest_stock_price: ⚠️ Error while getting from NATS: {e}")
            return None