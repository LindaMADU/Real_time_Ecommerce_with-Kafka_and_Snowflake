import json
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "host.docker.internal:29092"  # Update with your Kafka broker address
INPUT_TOPIC = "raw_events"  # Update with your Kafka topic name
OUTPUT_TOPIC ="clean_events"  # Update with your Kafka topic name
GROUP_ID = "silver_stream_processor"   # Update with your desired consumer group ID

VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",  # Start consuming from the earliest message if no offset is committed
    enable_auto_commit=False,  # Automatically commit offsets after processing messages
    key_deserializer=lambda k: k.decode("utf-8") if k else None,  # Decode the key from bytes to string
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))  # Decode the value from bytes to JSON
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,  # Serialize the key to bytes
    value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize the value to JSON and then to bytes
)

def is_valid_event(event):
    if not event.get("customer_id"):
        return False 
    
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    
    if event.get("amount") is None or event.get("amount") <= 0:
        return False
    
    if not event.get("currency"):
        return False
    
    if event.get("is_valid") is not True:
        return False
    return True

print("Starting Silver Stream Processor....")    # Log message to indicate that the stream processor has started

for message in consumer:
    key = message.key
    event = message.value
    
    if is_valid_event(event):
        producer.send(
            topic=OUTPUT_TOPIC,
            key=key,
            value=event
        )
        print(f"FORWARDED | key={key} | event_type={event['event_type']}")  # Log message to indicate that the event has been forwarded to the clean_events topic
    else:
        print(f"DROPPED | key={key} | reason=invalid")  # Log message to indicate that the event has been dropped
        
    consumer.commit()  # Commit the offset after processing the message