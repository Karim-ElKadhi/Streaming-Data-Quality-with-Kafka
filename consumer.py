import json
from kafka import KafkaConsumer, KafkaProducer
def is_valid(record):
    required = {"ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count"}

    if set(record.keys()) != required:
        return False, "Invalid fields"

    if not isinstance(record["ORIGIN_COUNTRY_NAME"], str) or not record["ORIGIN_COUNTRY_NAME"].strip():
        return False, "Invalid origin"

    if not isinstance(record["DEST_COUNTRY_NAME"], str) or not record["DEST_COUNTRY_NAME"].strip():
        return False, "Invalid destination"

    if not isinstance(record["count"], int) or record["count"] <= 0:
        return False, "Invalid count"

    return True, None


consumer = KafkaConsumer(
    'topic_csv',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)


for message in consumer:
    try:
        record = message.value
        if is_valid(record):
            print("VALID:", record)
        else:
            print("INVALID:", record)
    except Exception:
        print("INVALID JSON")