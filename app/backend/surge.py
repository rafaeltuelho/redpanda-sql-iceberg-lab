"""Surge button: produce a burst of trips from one zone into trips_raw (origin=surge).
They go through the trips-enrich pipeline like any other trip."""
import os
import random
from datetime import timedelta

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import settings
from .timeutil import ny_now

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "schemas", "trip_raw.avsc")
# Busy destinations: Midtown, Times Sq, UES, LES, FiDi, Williamsburg, JFK, LGA
DESTINATIONS = [161, 230, 237, 148, 87, 255, 132, 138, 162, 236, 79, 48]

_producer: Producer | None = None
_serialize: AvroSerializer | None = None


def _init() -> None:
    global _producer, _serialize
    sr = SchemaRegistryClient({"url": settings.sr_url,
                               "basic.auth.user.info": f"{settings.kafka_user}:{settings.kafka_pass}"})
    _serialize = AvroSerializer(sr, open(SCHEMA_FILE).read(),
                                conf={"auto.register.schemas": False, "use.latest.version": True})
    _producer = Producer({
        "bootstrap.servers": settings.bootstrap,
        "security.protocol": os.environ.get("RP_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": settings.sasl_mechanism,
        "sasl.username": settings.kafka_user,
        "sasl.password": settings.kafka_pass,
        "linger.ms": 20,
    })


def _trip(zone_id: int) -> dict:
    pickup = ny_now() - timedelta(milliseconds=random.randint(0, 3000))
    distance = round(random.uniform(0.6, 9.0), 2)
    minutes = distance * random.uniform(3.0, 6.0) + random.uniform(2, 6)
    fare = round(3.0 + 2.8 * distance + 0.6 * minutes, 2)
    tip = round(fare * random.choice([0, 0.15, 0.2, 0.25]), 2)
    return {
        "pickup_datetime": int(pickup.timestamp() * 1_000_000),
        "dropoff_datetime": int((pickup + timedelta(minutes=minutes)).timestamp() * 1_000_000),
        "passenger_count": random.choice([1, 1, 1, 2, 2, 3, 4]),
        "trip_distance": distance,
        "pickup_location_id": zone_id,
        "dropoff_location_id": random.choice(DESTINATIONS),
        "payment_type": random.choice([1, 1, 1, 2]),
        "fare_amount": fare,
        "tip_amount": tip,
        "total_amount": round(fare + tip + 4.5, 2),
        "origin": "surge",
    }


def surge(zone_id: int, count: int) -> int:
    if _producer is None:
        _init()
    ctx = SerializationContext(settings.raw_topic, MessageField.VALUE)
    for _ in range(count):
        _producer.produce(settings.raw_topic, value=_serialize(_trip(zone_id), ctx))
    remaining = _producer.flush(10)
    return count - remaining
