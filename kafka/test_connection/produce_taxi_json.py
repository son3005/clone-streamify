import csv
import os
from json import dumps
from kafka import KafkaProducer
from time import sleep
from datetime import datetime

KAFKA_ADDRESS= os.getenv("KAFKA_ADDRESS", "localhost:9092")
KAFKA_PORT= os.getenv("KAFKA_PORT", "9092")

producer= KafkaProducer(
    bootstrap_servers=f"{KAFKA_ADDRESS}:{KAFKA_PORT}",
    value_serializer=lambda v: dumps(v).encode("utf-8"),
    value_serializer= lambda v: dumps(v, default=str).encode("utf-8"))

file= open("data/rides.csv")
csvreader= csv.reader(file)
header= next(csvreader)
for row in csvreader:
    key= {"vendorId": int(row[0])}

    value = {"vendorId": int(row[0]),
            "passenger_count": int(row[3]),
            "trip_distance": float(row[4]),
            "pickup_location": int(row[7]),
            "dropoff_location": int(row[8]),
            "payment_type": int(row[9]),
            "total_amount": float(row[16]),
            "pickup_datetime": datetime.now()
            }

    producer.send("yellow_taxi_ride,json", key=key, value=value)
    print("producing")
    sleep(1)
    