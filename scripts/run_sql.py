"""
Run a .sql file (statements separated by ';') or a single query against Redpanda SQL.
Reads the same RP_SQL_* environment variables as the backend (see .env.example).

Usage:
    python scripts/run_sql.py scripts/sql/create_tables.sql
    python scripts/run_sql.py -c "SELECT count(*) FROM default_redpanda_catalog=>trips_enriched"
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import psycopg  # noqa: E402

from app.backend.config import settings  # noqa: E402

if len(sys.argv) == 3 and sys.argv[1] == "-c":
    statements = [sys.argv[2]]
else:
    text = open(sys.argv[1]).read()
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]

with psycopg.connect(settings.sql_conninfo(), autocommit=True, prepare_threshold=None) as conn:
    for stmt in statements:
        print(f"> {stmt.splitlines()[0]}")
        try:
            cur = conn.execute(stmt)
            if cur.description:
                print(" | ".join(c.name for c in cur.description))
                for row in cur.fetchall():
                    print(" | ".join(str(v) for v in row))
        except psycopg.Error as e:
            print(f"  ! {e.__class__.__name__}: {str(e).strip()}")
