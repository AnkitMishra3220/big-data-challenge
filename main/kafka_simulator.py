from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaSimulator")

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Allowed campaign IDs
campaign_ids = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140]


def generate_event():
    """Generate a random event message."""
    return {
        "view_id": random.randint(1000, 9999),
        "start_timestamp": (datetime.now() - timedelta(seconds=random.randint(0, 60))).isoformat(),
        "end_timestamp": (datetime.now() + timedelta(seconds=random.randint(10, 100))).isoformat(),
        "banner_id": random.randint(1, 10),
        "campaign_id": random.choice(campaign_ids),  # Select from specified campaign IDs
    }


def send_message():
    """Send a message to the Kafka topic 'view_log' with error handling."""
    event = generate_event()
    try:
        producer.send('view_log', event).get(timeout=10)  # Wait for send confirmation
        logger.info(f"Sent event: {event}")
    except Exception as e:
        logger.error(f"Failed to send event: {event} due to {e}")


# Continuously generate and send messages to Kafka
while True:
    send_message()
    time.sleep(1)  # Send a message every second
