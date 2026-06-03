import json
import random 
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
# Create a Kafka producer
BOOTSTRAP_SERVERS = "host.docker.internal:29092"  # Update with your Kafka broker address
Topic_NAME ="raw_events"  # Update with your Kafka topic name

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k  else None,                  # allows us to assign a key to each message, which can be used for partitioning and message ordering   
    value_serializer=lambda v: json.dumps(v).encode("utf-8")  # converts the message value to a JSON string and then encodes it to bytes, which is the format required by Kafka
    
    )

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PURCHASE"] # Invalid event types for testing

def random_timestamp_last_6_days():
    now = datetime.utcnow()
    past = now - timedelta(days=6)

    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_event():
    is_invalid = random.random() < 0.25  # 25% chance to generate an event 
    
    customer_id = f"CUST{random.randint(1, 5)}"   # Generate a random customer ID (CUST1 to CUST5)
    event_type = random.choice(EVENT_TYPES)  # Randomly select an event type from the valid list

    amount = round(random.uniform(10, 500), 2)   # Generate a random amount between 10 and 500, rounded to 2 decimal places
    currency = "USD"  # Set currency to USD for simplicity

    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id",
            "event_type",
            "amount",
            "currency"
        ])

    event = {
        "event_id": str(uuid.uuid4()),  # Generate a unique event ID
        "customer_id": None if invalid_field == "customer_id" else customer_id,
        "event_type": (random.choice(INVALID_EVENT_TYPES) if invalid_field == "event_type" else event_type),
        "amount": (round(random.uniform(-500, -10), 2) if invalid_field == "amount" else amount),
        "currency": None if invalid_field == "currency" else currency,
        "event_timestamp": random_timestamp_last_6_days().isoformat(),  #  Use ISO format for timestamps
        "is_valid": not is_invalid,
        "invalid_field": invalid_field
    }

    return event["customer_id"], event
print("Starting Kafka producer...")

while True:
    key,event = generate_event() # Generate a random event and its corresponding key
    
    producer.send(
        topic=Topic_NAME,
        key=key,
        value=event
    )
    
    print(f"produced event | key:{key} | valid={event['is_valid']}")
    
    time.sleep(1)  # Sleep for 1 second before producing the next event
