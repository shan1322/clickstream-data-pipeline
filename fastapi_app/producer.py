from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream')

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            security_protocol="SSL",
            ssl_context=__import__("ssl").create_default_context(),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
            metadata_max_age_ms=5000
        )
    return _producer

def send_event(event: dict):
    get_producer().send(
        KAFKA_TOPIC,
        key=event['user_id'],
        value=event
    )

def send_bulk_events(events: list):
    p = get_producer()
    for event in events:
        p.send(KAFKA_TOPIC, key=event['user_id'], value=event)
    p.flush()
