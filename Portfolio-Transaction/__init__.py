import os
from dotenv import load_dotenv
from logs.logger import Logger

## Logger init
logger_object = {
    'info': Logger("info"),
    'error':  Logger("error"),
    'success':  Logger("success"),
}

load_dotenv()
profile_mongodb_conventional_string=os.getenv("PRO_MONGODB_STRING")

matching_engine_api = os.getenv("MATCHING_ENGINE_API")
matching_engine_modify_order = os.getenv("MATCHING_ENGINE_MODIFY_ORDER")
matching_engine_cancel_order = os.getenv("MATCHING_ENGINE_CANCEL_ORDER")

redis_port = os.getenv("REDIS_PORT")
redis_host = os.getenv("REDIS_HOST")