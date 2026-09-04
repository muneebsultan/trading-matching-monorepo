import os
import asyncio
import json
from datetime import datetime, timedelta
import pytz
import nats
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv


class NATSDataCollector:
    def __init__(self):
        self.nats_url = os.getenv("NATS_URL")
        self.nats_user = os.getenv("NATS_USER")
        self.nats_password = os.getenv("NATS_PASSWORD")

        self.symbol_data = defaultdict(list)
        self.minute_history = []
        self.current_utc_minute = None
        self.nc = None

    async def connect_to_nats(self):
        try:
            self.nc = await nats.connect(
                servers=[self.nats_url],
                user=self.nats_user,
                password=self.nats_password,
                max_reconnect_attempts=3,
                reconnect_time_wait=2
            )
            print(f"[Connected] NATS: {self.nats_url}")
            return True
        except Exception as e:
            print(f"[Error] Connecting to NATS: {e}")
            return False

    async def disconnect(self):
        if self.nc:
            await self.nc.close()
            print("[Disconnected] NATS server")

    async def subscribe_to_subjects(self, subjects=None):
        if subjects is None:
            subjects = [">"]
        for subject in subjects:
            await self.nc.subscribe(subject, cb=self.handle_message)
            print(f"[Subscribed] to: {subject}")

    async def handle_message(self, msg):
        try:
            parsed = json.loads(msg.data.decode())
            utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)

            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        self.process_record(item, utc_now)
        except Exception as e:
            print(f"[Parse Error] {e}")

    def process_record(self, item, utc_time):
        symbol = item.get("symbol")
        if not symbol:
            return

        symbol = symbol.upper()
        minute_key = utc_time.strftime("%H:%M")
        symbol_minute_key = f"{symbol}_{minute_key}"
        item['timestamp_UTC'] = utc_time.strftime("%H:%M:%S.%f")[:-3]

        self.symbol_data[symbol_minute_key].append(item)

        if self.current_utc_minute != minute_key:
            self.cleanup_old_data(minute_key)
            self.current_utc_minute = minute_key

    def cleanup_old_data(self, current_minute_key):
        self.minute_history.append(current_minute_key)
        if len(self.minute_history) > 2:
            old_minute = self.minute_history.pop(0)
            keys_to_remove = [key for key in self.symbol_data if key.endswith(f"_{old_minute}")]
            for key in keys_to_remove:
                del self.symbol_data[key]
            print(f"[Cleanup] Removed data for old minute: {old_minute}")

    def calculate_ohlcv(self, symbol_minute_key, records, utc_time):
        if not records:
            return None
        prices = [float(r['price']) for r in records]
        sizes = [int(r['size']) for r in records]
        return {
            "symbol": symbol_minute_key.split('_')[0],
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(sizes),
            "source": "NASDAQ",
            "ts": utc_time.replace(second=0, microsecond=0)
        }

    def get_current_ohlcv(self, symbol_minute_key):
        if symbol_minute_key in self.symbol_data:
            records = self.symbol_data[symbol_minute_key]
            if records:
                first_record = records[0]
                if 'timestamp_UTC' in first_record:
                    utc_time_str = first_record['timestamp_UTC']
                    time_parts = utc_time_str.split(':')
                    hour = int(time_parts[0])
                    minute = int(time_parts[1])
                    second_parts = time_parts[2].split('.')
                    second = int(second_parts[0])
                    microsecond = int(second_parts[1]) * 1000 if len(second_parts) > 1 else 0

                    today = datetime.now(pytz.UTC).date()
                    utc_time = datetime.combine(today, datetime.min.time().replace(
                        hour=hour, minute=minute, second=second, microsecond=microsecond
                    )).replace(tzinfo=pytz.UTC)

                    return self.calculate_ohlcv(symbol_minute_key, records, utc_time)
        return None

    # def get_latest_ohlcv_by_symbol(self, symbol):
    #     symbol = symbol.upper()
    #     relevant_keys = sorted([k for k in self.symbol_data if k.startswith(f"{symbol}_")])
    #     if not relevant_keys:
    #         return None
    #     latest_key = relevant_keys[-1]
    #     return self.get_current_ohlcv(latest_key)
    def get_latest_ohlcv_by_symbol(self, symbol):
        symbol = symbol.upper()
        all_keys = list(self.symbol_data.keys())
        print(f"[DEBUG] All keys in symbol_data: {all_keys}")
        
        relevant_keys = sorted([k for k in all_keys if k.startswith(f"{symbol}_")])
        print(f"[DEBUG] Relevant keys for symbol {symbol}: {relevant_keys}")
        
        if not relevant_keys:
            return None
        latest_key = relevant_keys[-1]
        print(f"[DEBUG] Using latest key: {latest_key}")
        return self.get_current_ohlcv(latest_key)


    def get_all_current_ohlcv(self):
        ohlcv_data = {}
        for key in self.symbol_data:
            ohlcv = self.get_current_ohlcv(key)
            if ohlcv:
                ohlcv_data[key] = ohlcv
        return ohlcv_data


async def main():
    collector = NATSDataCollector()

    try:
        if not await collector.connect_to_nats():
            return

        await collector.subscribe_to_subjects()
        print("[INFO] Collector running... press Ctrl+C to stop.")

        while True:
            await asyncio.sleep(10)
            ohlcv = collector.get_latest_ohlcv_by_symbol("AAPL")
            if ohlcv:
                print("[Latest OHLCV]", ohlcv)

    except KeyboardInterrupt:
        print("\n[Shutdown] Interrupted")
    finally:
        await collector.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
