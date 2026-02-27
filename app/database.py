import sqlite3
import contextlib
from datetime import datetime
from pathlib import Path

DB_PATH = Path("/data/wind_monitor.db")


def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextlib.contextmanager
def db():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS observations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id  TEXT NOT NULL,
                station_name TEXT NOT NULL,
                fetched_at  TEXT NOT NULL,
                wind_speed  REAL,
                wind_gust   REAL,
                wind_dir    TEXT,
                pressure    REAL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_obs_station_time
            ON observations (station_id, fetched_at)
        """)


def insert_observation(station_id: str, station_name: str, data: dict):
    with db() as conn:
        conn.execute("""
            INSERT INTO observations
                (station_id, station_name, fetched_at,
                 wind_speed, wind_gust, wind_dir, pressure)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            station_id,
            station_name,
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
            data.get("wind_speed"),
            data.get("wind_gust"),
            data.get("wind_dir"),
            data.get("pressure"),
        ))


def get_observations(station_id: str, hours: int = 48):
    with db() as conn:
        rows = conn.execute("""
            SELECT * FROM observations
            WHERE station_id = ?
              AND fetched_at >= datetime('now', ? || ' hours')
            ORDER BY fetched_at ASC
        """, (station_id, f"-{hours}")).fetchall()
    return [dict(r) for r in rows]


def get_latest(station_id: str):
    with db() as conn:
        row = conn.execute("""
            SELECT * FROM observations
            WHERE station_id = ?
            ORDER BY fetched_at DESC
            LIMIT 1
        """, (station_id,)).fetchone()
    return dict(row) if row else None


def get_all_latest():
    with db() as conn:
        rows = conn.execute("""
            SELECT o.*
            FROM observations o
            INNER JOIN (
                SELECT station_id, MAX(fetched_at) AS max_time
                FROM observations
                GROUP BY station_id
            ) latest ON o.station_id = latest.station_id
                       AND o.fetched_at = latest.max_time
        """).fetchall()
    return [dict(r) for r in rows]
