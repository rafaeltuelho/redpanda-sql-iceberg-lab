"""
Step 4 — Produce the sorted trips into a Redpanda topic as Avro, using the
Schema Registry wire format (required by redpanda.iceberg.mode=value_schema_id_prefix).

Usage:
    pip install "confluent-kafka[avro]" pyarrow
    export RP_BOOTSTRAP="<bootstrap-url>:9092"
    export RP_SR_URL="https://<schema-registry-url>"
    export RP_USER="taxi-lab" RP_PASS="<password>"
    python produce.py trips_sorted.parquet trips
"""
import os
import sys
import time
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

PARQUET_FILE = sys.argv[1] if len(sys.argv) > 1 else "trips_sorted.parquet"
TOPIC = sys.argv[2] if len(sys.argv) > 2 else "trips"

SCHEMA = """
{
  "type": "record", "name": "Trip", "namespace": "nyc.taxi",
  "fields": [
    {"name": "pickup_datetime",     "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "dropoff_datetime",    "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "passenger_count",     "type": ["null", "int"], "default": null},
    {"name": "trip_distance",       "type": "double"},
    {"name": "pickup_location_id",  "type": "int"},
    {"name": "dropoff_location_id", "type": "int"},
    {"name": "payment_type",        "type": "int"},
    {"name": "fare_amount",         "type": "double"},
    {"name": "tip_amount",          "type": "double"},
    {"name": "total_amount",        "type": "double"}
  ]
}
"""

user, pwd = os.environ["RP_USER"], os.environ["RP_PASS"]

sr = SchemaRegistryClient({"url": os.environ["RP_SR_URL"],
                           "basic.auth.user.info": f"{user}:{pwd}"})
# Auto-registers the schema under subject "<topic>-value" on first use
serialize = AvroSerializer(sr, SCHEMA)

producer = Producer({
    "bootstrap.servers": os.environ["RP_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    # Must match the mechanism the user was created with in Redpanda Cloud
    "sasl.mechanism": os.environ.get("RP_SASL_MECHANISM", "SCRAM-SHA-256"),
    "sasl.username": user,
    "sasl.password": pwd,
    "linger.ms": 50,
    "batch.size": 1_000_000,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 500_000,
})

ctx = SerializationContext(TOPIC, MessageField.VALUE)
sent, start = 0, time.time()

for batch in pq.ParquetFile(PARQUET_FILE).iter_batches(batch_size=50_000):
    for row in batch.to_pylist():          # rows arrive in pickup_datetime order
        while True:
            try:
                producer.produce(TOPIC, value=serialize(row, ctx))  # null key
                break
            except BufferError:
                producer.poll(0.5)         # local queue full: let it drain
        sent += 1
    producer.poll(0)
    print(f"{sent:,} rows sent ({sent / (time.time() - start):,.0f} rows/s)")

producer.flush()
print(f"Done: {sent:,} rows in {time.time() - start:,.0f}s")
