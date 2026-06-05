from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    acks='all',
    retries=3,
    max_in_flight_requests_per_connection=1
)

def send_event(event: dict):
    producer.send(
        KAFKA_TOPIC,
        key=event['user_id'],
        value=event
    )

def send_bulk_events(events: list):
    for event in events:
        send_event(event)
    producer.flush()
