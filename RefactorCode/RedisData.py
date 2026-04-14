import redis
from datetime import datetime
from __init__ import logger_object, redis_port, redis_host

def get_latest_stock_price(KEY_TO_FETCH):
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    try:
        key = f"{KEY_TO_FETCH.upper()}_NASDAQ"
        if r.exists(key):
            value = r.get(key)

        else:
            logger_object['error'].log(f"Key '{KEY_TO_FETCH}' does not exist in Redis.")
            value = None
        
        return value
    except redis.RedisError as e:
        logger_object['error'].log(f"Error fetching stock price: {e}")