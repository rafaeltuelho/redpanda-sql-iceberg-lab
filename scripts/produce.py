"""
Step 4 — Produce the sorted trips into `trips_raw` as Avro (Schema Registry wire
format), tagged origin=history. The trips-enrich Connect pipeline enriches them into
the Iceberg-enabled `trips_enriched` topic.

Usage:
    pip install "confluent-kafka[avro]" pyarrow
    export RP_BOOTSTRAP="<bootstrap-url>:9092"
    export RP_SR_URL="https://<schema-registry-url>"
    export RP_USER="taxi-lab" RP_PASS="<password>"
    python scripts/produce.py trips_sorted.parquet trips_raw
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
TOPIC = sys.argv[2] if len(sys.argv) > 2 else "trips_raw"

SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schemas", "trip_raw.avsc")
SCHEMA = open(SCHEMA_FILE).read()

user, pwd = os.environ["RP_USER"], os.environ["RP_PASS"]

sr = SchemaRegistryClient({"url": os.environ["RP_SR_URL"],
                           "basic.auth.user.info": f"{user}:{pwd}"})
# scripts/setup_cluster.sh registers the schema under "<topic>-value"
serialize = AvroSerializer(sr, SCHEMA, conf={"auto.register.schemas": False, "use.latest.version": True})

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
        row["origin"] = "history"
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
