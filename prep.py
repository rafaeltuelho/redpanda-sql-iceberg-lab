"""
Step 2 — Download, clean, slim down and SORT the NYC Yellow Taxi data.

Sorting by pickup time is the key detail: Redpanda writes Parquet files in
the order records arrive, so time-ordered input = each Iceberg data file covers
a narrow time range = the min/max stats can prune files at query time.

Usage:
    pip install duckdb pyarrow
    python prep.py
"""
import os
import urllib.request
import duckdb

MONTHS = ["2022-12", "2023-01", "2023-02"]   # ~9M rows total; add more for a bigger run
BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
os.makedirs("raw", exist_ok=True)

for m in MONTHS:
    path = f"raw/yellow_tripdata_{m}.parquet"
    if not os.path.exists(path):
        print(f"Downloading {m} ...")
        urllib.request.urlretrieve(BASE.format(m=m), path)

duckdb.sql("""
COPY (
    SELECT
        epoch_us(tpep_pickup_datetime)            AS pickup_datetime,   -- int micros (Avro timestamp-micros)
        epoch_us(tpep_dropoff_datetime)           AS dropoff_datetime,
        CAST(passenger_count AS INTEGER)          AS passenger_count,   -- nullable
        COALESCE(trip_distance, 0)::DOUBLE        AS trip_distance,
        COALESCE(PULocationID, 0)::INTEGER        AS pickup_location_id,
        COALESCE(DOLocationID, 0)::INTEGER        AS dropoff_location_id,
        COALESCE(payment_type, 0)::INTEGER        AS payment_type,
        COALESCE(fare_amount, 0)::DOUBLE          AS fare_amount,
        COALESCE(tip_amount, 0)::DOUBLE           AS tip_amount,
        COALESCE(total_amount, 0)::DOUBLE         AS total_amount
    FROM read_parquet('raw/*.parquet')
    -- TLC files contain stray timestamps from other years; keep only our window
    WHERE tpep_pickup_datetime >= TIMESTAMP '2022-12-01'
      AND tpep_pickup_datetime <  TIMESTAMP '2023-03-01'
      AND total_amount >= 0
      AND trip_distance >= 0
    ORDER BY tpep_pickup_datetime                       -- <-- the important line
) TO 'trips_sorted.parquet' (FORMAT PARQUET);
""")

print(duckdb.sql("""
    SELECT COUNT(*) AS rows,
           to_timestamp(MIN(pickup_datetime)/1e6) AS first_pickup,
           to_timestamp(MAX(pickup_datetime)/1e6) AS last_pickup
    FROM 'trips_sorted.parquet'
"""))
