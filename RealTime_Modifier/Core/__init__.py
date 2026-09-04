from Core.logger import Logger
import os

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

## Logger init
logger_object = {
    'info': Logger("info"),
    'error': Logger("error"),
    'success': Logger("success"),
}

# Test log to verify logger is working
logger_object['info'].log("🟢 Logger initialized successfully")