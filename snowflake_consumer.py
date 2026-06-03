import json
from kafka import KafkaConsumer
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas  # to write the data to snowflake using pandas dataframe
import pandas as pd # to create a dataframe from the Kafka messages

BOOTSTRAP_SERVERS = "host.docker.internal:29092"  # Update with your Kafka broker address
TOPIC_NAME = "clean_events"  # Update with your Kafka topic name
GROUP_ID = "snowflake-loader"  # Update with your desired consumer group ID

SNOWFLAKE_CONFIG = {
    "user": "linda",
     "password": "Schools4me&you",
     "account": "vd96976.us-east-2.aws",
     "warehouse": "COMPUTE_WH",
     "database": "KAFKA_DB",
     "schema": "STREAMING"
     
}

BATCH_SIZE = 10   # Number of records to batch before writing to Snowflake

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",  # Start consuming from the earliest message if no offset is committed
    enable_auto_commit=False,  # Automatically commit offsets after processing messages
    key_deserializer=lambda k: k.decode("utf-8") if k else None,  # Decode the key from bytes to string
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))  # Decode the value from bytes to JSON
)

sf_connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG) # Establish a connection to Snowflake using the provided configuration

print("Connected to Snowflake")
print("Starting kafka -> Snowflake Loader...")

buffer = []  # Buffer to hold messages before writing to Snowflake

def flush_to_snowflake(records):
    df = pd.DataFrame(records)  # Convert the list of records to a pandas DataFrame
    df.columns = [c.upper() for c in df.columns]  # Convert column names to uppercase to match Snowflake's case sensitivity
    
    success, nchunks, nrows, output = write_pandas(
    conn=sf_connection,
    df=df,
    table_name="KAFKA_EVENTS_SILVER"
        )                               # Write the DataFrame to Snowflake using the write_pandas function
    
    if not success:
        raise Exception("Snowflake insert failed")  # Raise an exception if the insert operation failed
    
    print(f"Inserted {nrows} rows into Snowflake")  # Log the number of rows inserted into Snowflake
    
for message in consumer:
    event = message.value
    buffer.append({
        "event_id": event["event_id"],
        "customer_id": event["customer_id"],
        "event_type": event["event_type"],
        "amount": event["amount"],
        "currency": event["currency"],
        "event_timestamp": event["event_timestamp"]    
    })                                                    # Add the event to the buffer
    
    if len(buffer) >= BATCH_SIZE:  # Check if the buffer has reached the specified batch size
        try:
            flush_to_snowflake(buffer)  # Flush the buffered records to Snowflake
            consumer.commit()  # Commit the offsets after processing the batch
            buffer.clear()  # Clear the buffer after successful insertion
        except Exception as e:
            print(f"ERROR inserting batch: {e}")  # Log any errors that occur during the insertion process
        