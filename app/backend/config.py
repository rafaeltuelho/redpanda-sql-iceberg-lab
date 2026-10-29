"""Settings from environment variables (and a .env file at the repo root, if present)."""
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str) -> str:
    if not IDENT.match(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return name


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class Settings:
    # Kafka + Schema Registry (used by the surge producer)
    bootstrap: str = os.environ.get("RP_BOOTSTRAP", "")
    sr_url: str = os.environ.get("RP_SR_URL", "")
    kafka_user: str = os.environ.get("RP_USER", "")
    kafka_pass: str = os.environ.get("RP_PASS", "")
    sasl_mechanism: str = os.environ.get("RP_SASL_MECHANISM", "SCRAM-SHA-256")
    raw_topic: str = os.environ.get("RAW_TOPIC", "trips_raw")

    # Redpanda SQL
    sql_host: str = os.environ.get("RP_SQL_HOST", "")
    sql_port: int = int(os.environ.get("RP_SQL_PORT", "5432"))
    sql_db: str = os.environ.get("RP_SQL_DB", "oxla")
    sql_user: str = os.environ.get("RP_SQL_USER", "")
    sql_password: str = os.environ.get("RP_SQL_PASSWORD", "")
    sql_auth: str = os.environ.get("RP_SQL_AUTH", "scram")  # scram | client_secret | bearer
    catalog: str = _ident(os.environ.get("RP_SQL_CATALOG", "default_redpanda_catalog"))
    trips_table: str = _ident(os.environ.get("TRIPS_TABLE", "trips_enriched"))
    quarantine_table: str = _ident(os.environ.get("QUARANTINE_TABLE", "trips_quarantine"))
    weather_table: str = _ident(os.environ.get("WEATHER_TABLE", "weather_hourly"))

    # Demo behaviour
    history_from: datetime = field(default_factory=lambda: _ts(os.environ.get("HISTORY_FROM", "2022-12-01T00:00:00")))
    history_to: datetime = field(default_factory=lambda: _ts(os.environ.get("HISTORY_TO", "2023-03-01T00:00:00")))
    iceberg_lag_ms: int = int(os.environ.get("ICEBERG_LAG_MS", "60000"))
    replay_weeks: int = int(os.environ.get("REPLAY_WEEKS", "4"))
    live_poll_s: float = float(os.environ.get("LIVE_POLL_S", "2"))

    def sql_conninfo(self) -> str:
        options = {"client_secret": "auth_method=client_secret", "bearer": "auth_method=bearer"}.get(self.sql_auth)
        kwargs = dict(host=self.sql_host, port=self.sql_port, dbname=self.sql_db,
                      user=self.sql_user or "ignored", password=self.sql_password, sslmode="require")
        if options:
            kwargs["options"] = options
        return make_conninfo(**kwargs)

    def table(self, name: str) -> str:
        return f"{self.catalog}=>{name}"


settings = Settings()
