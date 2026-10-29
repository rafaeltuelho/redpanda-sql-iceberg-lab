"""SQL used by the API. Every trips query carries a pickup_datetime range so Iceberg
file min/max stats can prune files. Times are returned as epoch ms (NYC wall clock)."""
from .config import settings

T = settings.table(settings.trips_table)
Q = settings.table(settings.quarantine_table)
W = settings.table(settings.weather_table)

BUCKET = "timestamp_micros((unix_micros(pickup_datetime) / %(bucket_us)s::bigint) * %(bucket_us)s::bigint)"

VOLUME = f"""
SELECT t, count(*) AS trips FROM (
  SELECT unix_millis({BUCKET}) AS t
  FROM {T}
  WHERE pickup_datetime >= %(from)s AND pickup_datetime < %(to)s
) s GROUP BY t ORDER BY t
"""

FRAMES = f"""
SELECT t, zone, count(*) AS trips, avg(total_amount) AS avg_total FROM (
  SELECT unix_millis({BUCKET}) AS t, pickup_location_id AS zone, total_amount
  FROM {T}
  WHERE pickup_datetime >= %(from)s AND pickup_datetime < %(to)s
) s GROUP BY t, zone
"""

ARCS = f"""
SELECT unix_millis(pickup_datetime), unix_millis(dropoff_datetime),
       pickup_location_id, dropoff_location_id, total_amount, origin
FROM {T}
WHERE pickup_datetime >= %(from)s AND pickup_datetime < %(to)s
  AND ((unix_micros(pickup_datetime) / 1000000) %% %(sample)s::bigint) = 0  -- 1-in-N by pickup second
ORDER BY pickup_datetime
LIMIT %(limit)s
"""

# Weather rows are deduplicated per hour: the archive pull reruns if the pipeline restarts
WEATHER = f"""
SELECT unix_millis(obs_hour), max(temp_c), max(precip_mm), max(snow_cm), max(wind_kmh), max(weather_code)
FROM {W}
WHERE obs_hour >= %(from)s AND obs_hour < %(to)s
GROUP BY obs_hour ORDER BY obs_hour
"""

RAIN = f"""
SELECT sky, count(*) AS trips, count(DISTINCT obs_hour) AS hours,
       avg(tip_pct) AS avg_tip_pct, avg(duration_min) AS avg_duration, avg(total_amount) AS avg_total
FROM (
  SELECT CASE WHEN w.snow_cm > 0 THEN 'snow' WHEN w.precip_mm > 0.2 THEN 'rain' ELSE 'dry' END AS sky,
         w.obs_hour, t.tip_pct, t.duration_min, t.total_amount
  FROM {T} t
  JOIN (SELECT obs_hour, max(precip_mm) AS precip_mm, max(snow_cm) AS snow_cm
        FROM {W} GROUP BY obs_hour) w
    ON date_trunc('hour', t.pickup_datetime) = w.obs_hour
  WHERE t.pickup_datetime >= %(from)s AND t.pickup_datetime < %(to)s
) s GROUP BY sky
"""

THEN_VS_NOW = f"""
SELECT period, origin, count(*) AS trips, avg(total_amount) AS avg_total, avg(tip_pct) AS avg_tip_pct,
       sum(CASE WHEN is_airport THEN 1 ELSE 0 END) AS airport_trips
FROM (
  SELECT CASE WHEN pickup_datetime >= %(now_from)s THEN 'now' ELSE 'then' END AS period,
         origin, total_amount, tip_pct, is_airport
  FROM {T}
  WHERE (pickup_datetime >= %(now_from)s AND pickup_datetime < %(now_to)s)
     OR (pickup_datetime >= %(then_from)s AND pickup_datetime < %(then_to)s)
) s GROUP BY period, origin
"""

QUALITY = f"""
SELECT reason, origin, count(*) AS trips FROM {Q} GROUP BY reason, origin ORDER BY trips DESC
"""

# Live mode. (redpanda).partition/offset identify a record, so the poller can deduplicate
# rows it has already sent; LIVE_RECENT_FALLBACK is used if struct access isn't available.
LIVE_RECENT = f"""
SELECT (redpanda).partition, (redpanda).offset,
       unix_millis(pickup_datetime), unix_millis(dropoff_datetime),
       pickup_location_id, dropoff_location_id, total_amount, origin
FROM {T}
WHERE pickup_datetime >= %(since)s
"""

LIVE_RECENT_FALLBACK = f"""
SELECT unix_millis(pickup_datetime), unix_millis(dropoff_datetime),
       pickup_location_id, dropoff_location_id, total_amount, origin
FROM {T}
WHERE pickup_datetime >= %(since)s
"""

LIVE_ZONES = f"""
SELECT pickup_location_id, count(*) AS trips
FROM {T}
WHERE pickup_datetime >= %(since)s
GROUP BY pickup_location_id
"""
