import os
import threading
import time
import json
from fastapi import FastAPI, HTTPException, BackgroundTasks
from kafka import KafkaProducer
from kafka.errors import KafkaError
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from __init__ import logger_object  # Custom logger import

app = FastAPI()
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")

# Order Topics
TOPIC_MAPPING = {
    "placed": os.getenv("TOPIC_PLACED", "Stocks-Order-Placed"),
    "modified": os.getenv("TOPIC_MODIFIED", "Stocks-Order-Modified"),
    "cancelled": os.getenv("TOPIC_CANCELLED", "Stocks-Order-Cancelled"),
}

HEARTBEAT_TOPIC = "heartbeat_topic"

# Kafka Producer Singleton with Lock
producer = None
producer_lock = threading.Lock()


def get_kafka_producer():
    """Returns a singleton Kafka producer instance in a thread-safe manner."""
    global producer
    with producer_lock:  # Ensure thread safety
        if producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: json.dumps(k).encode("utf-8") if k else None,
                    linger_ms=5,  # Small delay to batch messages
                    retries=5,
                    max_in_flight_requests_per_connection=5,
                    request_timeout_ms=10000,  # Reduced timeout to avoid long hangs
                    connections_max_idle_ms=3600000,
                )
                logger_object["info"].log("✅ Kafka Producer initialized successfully")
            except KafkaError as e:
                logger_object["error"].log(f"❌ Kafka Producer initialization failed: {str(e)}")
                producer = None
    return producer


@app.get("/")
def root():
    return {"message": "Kafka Order Producer is running"}


def send_event_to_kafka(topic: str, key: dict, value: dict):
    """Handles publishing events asynchronously to Kafka."""
    global producer

    kafka_producer = get_kafka_producer()
    if kafka_producer is None:
        logger_object["error"].log("❌ Kafka Producer is unavailable")
        return {"error": "Kafka Producer unavailable"}

    try:
        # Send the message and wait for acknowledgment
        future = kafka_producer.send(topic, key=key, value=value)
        # Wait for the message to be delivered (with timeout)
        record_metadata = future.get(timeout=10)
        
        logger_object["info"].log(f"✅ Event published to {topic}: Partition {record_metadata.partition}, Offset {record_metadata.offset}")
        return {
            "message": "Event published successfully", 
            "topic": topic, 
            "data": value,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset
        }
    except KafkaError as e:
        logger_object["error"].log(f"❌ Kafka error: {str(e)}")
        # Attempt to reconnect producer on error
        with producer_lock:
            if producer is not None:
                producer.close()
                producer = None
        return {"error": "Failed to publish event to Kafka"}
    except Exception as e:
        logger_object["error"].log(f"❌ Unexpected error: {str(e)}")
        return {"error": "Unexpected error while publishing to Kafka"}


@app.post("/publish_order/")
def publish_order(order_event: dict, background_tasks: BackgroundTasks):
    """Publishes an order event to Kafka asynchronously."""
    trade_id = order_event.get("trade_id")
    user_id = order_event.get("user_id")
    
    if not trade_id or not user_id :
        raise HTTPException(status_code=400, detail="Missing required fields: trade_id, user_id")

    topic = "Stocks-Order-Placed"
    message_key = {"user_id": user_id, "trade_id": trade_id}
    background_tasks.add_task(send_event_to_kafka, topic, message_key, order_event)

    return {"message": "Order event processing in background", "topic": topic, "data": order_event}


@app.post("/modify_order/")
def modify_order(order_event: dict, background_tasks: BackgroundTasks):
    """Modifies an existing order."""
    trade_id = order_event.get("trade_id")

    if not trade_id:
        raise HTTPException(status_code=400, detail="Missing required fields: trade_id")

    order_event["action"] = "Modified"
    topic = TOPIC_MAPPING["modified"]
    message_key = {"trade_id": trade_id}

    background_tasks.add_task(send_event_to_kafka, topic, message_key, order_event)

    return {"message": "Order modification processing in background", "topic": topic, "data": order_event}


@app.post("/delete_order/")
def delete_order(order_event: dict, background_tasks: BackgroundTasks):
    """Deletes an existing order."""
    trade_id = order_event.get("trade_id")

    if not trade_id:
        raise HTTPException(status_code=400, detail="Missing required fields: trade_id")

    order_event["action"] = "Deleted"
    topic = TOPIC_MAPPING["modified"]
    message_key = {"trade_id": trade_id}

    background_tasks.add_task(send_event_to_kafka, topic, message_key, order_event)

    return {"message": "Order deletion processing in background", "topic": topic, "data": order_event}


# 🔹 Optimized Async Heartbeat Function
def send_heartbeat():
    """Sends periodic heartbeat messages asynchronously."""
    global producer
    while True:
        try:
            kafka_producer = get_kafka_producer()
            if kafka_producer is None:
                continue  # Skip sending if producer is down

            heartbeat_message = {"heartbeat": "keep-alive", "timestamp": time.time()}
            kafka_producer.send(HEARTBEAT_TOPIC, value=heartbeat_message)
            logger_object["info"].log(f"💓 Sent keep-alive heartbeat")
        except KafkaError as e:
            logger_object["error"].log(f"⚠️ Heartbeat failed: {str(e)}")
            with producer_lock:
                if producer is not None:
                    producer.close()
                    producer = None
        time.sleep(30)  # Send heartbeat every 30 seconds


# Start async heartbeat thread
heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
heartbeat_thread.start()

# Run FastAPI
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8777, timeout_keep_alive=int(os.getenv("API_TIMEOUT", 600)))
