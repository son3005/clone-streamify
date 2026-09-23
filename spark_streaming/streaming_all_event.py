# Run the script using the following comand
# spark-submit /
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_all_event.py

import os
from streaming_fuctions import *
from schema import schema

# Kafka Topics
LISTEN_EVENTS_TOPIC= "listen_events"
PAGE_VIEW_EVENTS_TOPIC= "page_view_events"
AUTH_EVENTS_TOPIC= "auth_events"

KAFKA_PORT= os.getenv("KAFKA_PORT", 9092)

KAFKA_ADDRESS= os.getenv("KAFKA_ADDRESS","localhost")
GCP_GCS_BUCKET= os.getenv("GCP_GCS_BUCKET", "streamify")
GCS_STORAGE_PATH= f"gs://{GCP_GCS_BUCKET}"

# initialize a spark session
spark= create_or_get_spark_session("Eventism Stream")
spark.streams.resetTerminated()
# listen events stream
listen_events = create_kafka_read_stream(
    spark=spark,
    kafka_address=KAFKA_ADDRESS,
    kafka_port=KAFKA_PORT,
    topic=LISTEN_EVENTS_TOPIC,
    starting_offset="earliest"
)
listen_events = process_stream(
    stream=listen_events,
    stream_schema=schema[LISTEN_EVENTS_TOPIC],
    topic=LISTEN_EVENTS_TOPIC
)

# page view stream
page_view_events= create_kafka_read_stream(
    spark=spark,
    kafka_address=KAFKA_ADDRESS,
    kafka_port=KAFKA_PORT,
    topic=PAGE_VIEW_EVENTS_TOPIC,
    starting_offset="earliest"
)
page_view_events = process_stream(
    stream=page_view_events,
    stream_schema=schema[PAGE_VIEW_EVENTS_TOPIC],
    topic=PAGE_VIEW_EVENTS_TOPIC
)

# auth stream
auth_events= create_kafka_read_stream(
    spark=spark,
    kafka_address=KAFKA_ADDRESS,
    kafka_port=KAFKA_PORT,
    topic=AUTH_EVENTS_TOPIC,
    starting_offset="earliest"
)
auth_events = process_stream(
    stream=auth_events,
    stream_schema=schema[AUTH_EVENTS_TOPIC],
    topic=AUTH_EVENTS_TOPIC
)

# write a file to storage every 2 minutes in parquet format
listen_events_write= create_file_write_stream(
    stream=listen_events,
    storage_path= f"{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}",
    checkpoint_path= f"{GCS_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}",
    trigger="120 seconds",
    output_mode="append",
    file_format="parquet"
)
page_view_page_events_write= create_file_write_stream(
    stream=page_view_events,
    storage_path= f"{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
    checkpoint_path= f"{GCS_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}",
    trigger="120 seconds",
    output_mode="append",
    file_format="parquet"
)
auth_events_write= create_file_write_stream(
    stream=auth_events,
    storage_path= f"{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}",
    checkpoint_path= f"{GCS_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}",
    trigger="120 seconds",
    output_mode="append",
    file_format="parquet"
)

# start the stream
listen_events_write.start()
page_view_page_events_write.start()
auth_events_write.start()

spark.streams.awaitAnyTermination()
