import json
from kafka import KafkaProducer

TOPIC = "topic_csv"
FILE_PATH = "flights-summary.json"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

with open(FILE_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

for record in data:
    producer.send(TOPIC, record)
    print("Sent:", record)

producer.flush()
producer.close()