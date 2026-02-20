#!/usr/bin/env python3
"""
SensorFor Cloud API downloader
Downloads sensor measurements periodically and stores them in a SQLite database.

Usage:
    python sensorfor_downloader.py              # run scheduler (weekly)
    python sensorfor_downloader.py --run-now    # immediate download, then exit
    python sensorfor_downloader.py --status     # show last 20 download_log rows
    python sensorfor_downloader.py --query DEV  # show last 10 records for alias
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, Optional

import json

import requests
import schedule

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — edit this section to match your setup
# ──────────────────────────────────────────────────────────────────────────────

# Path to JSON file with device definitions (see devices.json).
# Set to None to use the DEVICES list below instead.
DEVICES_FILE: str | None = "devices.json"

# Fallback device list — used only when DEVICES_FILE is None.
DEVICES: list[dict] = [
    {
        "device_id": "12345",         # Unique ID found on the device label
        "alias": "office_sensor",     # Human-readable name (used in --query)
        "field_names": [              # Names for measurement fields (index 7+)
            "temperature",
            "humidity",
            "co2",
        ],
    },
]

# Download parameters
LINES: int = 960    # Records to fetch per download (API max: 960)
ZOOM: int = 1       # Time resolution: 1 = highest, 20 = lowest

# Scheduler: weekly on this day and time (24 h clock)
SCHEDULE_DAY: str = "monday"   # monday … sunday
SCHEDULE_TIME: str = "02:00"   # HH:MM

# File paths
DB_PATH: str = "sensorfor.db"
LOG_PATH: str = "sensorfor.log"

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

API_URL = "https://www.sensorfor.com/cloud/m2m_data_get.php"

API_ERRORS: frozenset[str] = frozenset(
    {
        "Device does not exist.",
        "API is not enabled.",
        "Wrong number of lines.",
        "Wrong data zoom.",
        "File does not exist.",
        "There is no data.",
    }
)

# ──────────────────────────────────────────────────────────────────────────────
# DEVICE LOADER
# ──────────────────────────────────────────────────────────────────────────────

def load_devices() -> list[dict]:
    """Return device list from DEVICES_FILE (JSON) or fall back to DEVICES."""
    if DEVICES_FILE is None:
        return DEVICES

    try:
        with open(DEVICES_FILE, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            raise ValueError("Top-level JSON value must be a list.")
        return data
    except FileNotFoundError:
        raise SystemExit(f"Device file not found: '{DEVICES_FILE}'. Create it or set DEVICES_FILE = None.")
    except (json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"Invalid device file '{DEVICES_FILE}': {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────

def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("sensorfor")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


log = _setup_logging()

# ──────────────────────────────────────────────────────────────────────────────
# DATABASE
# ──────────────────────────────────────────────────────────────────────────────

@contextmanager
def db_conn() -> Generator[sqlite3.Connection, None, None]:
    """Open a connection, commit on success, roll back on error, always close."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices (
                device_id  TEXT PRIMARY KEY,
                alias      TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS measurements (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id  TEXT    NOT NULL REFERENCES devices(device_id),
                timestamp  TEXT    NOT NULL,
                raw_line   TEXT    NOT NULL,
                UNIQUE (device_id, timestamp)
            );

            CREATE TABLE IF NOT EXISTS measurement_fields (
                measurement_id INTEGER NOT NULL REFERENCES measurements(id),
                field_name     TEXT    NOT NULL,
                field_value    REAL,
                PRIMARY KEY (measurement_id, field_name)
            );

            CREATE TABLE IF NOT EXISTS download_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id     TEXT    NOT NULL,
                downloaded_at TEXT    NOT NULL DEFAULT (datetime('now')),
                rows_fetched  INTEGER NOT NULL DEFAULT 0,
                rows_inserted INTEGER NOT NULL DEFAULT 0,
                status        TEXT    NOT NULL
            );
            """
        )
    log.debug("Database ready: %s", DB_PATH)


def _upsert_device(conn: sqlite3.Connection, device_id: str, alias: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO devices (device_id, alias) VALUES (?, ?)",
        (device_id, alias),
    )


def _log_run(
    conn: sqlite3.Connection,
    device_id: str,
    rows_fetched: int,
    rows_inserted: int,
    status: str,
) -> None:
    conn.execute(
        """
        INSERT INTO download_log (device_id, rows_fetched, rows_inserted, status)
        VALUES (?, ?, ?, ?)
        """,
        (device_id, rows_fetched, rows_inserted, status),
    )

# ──────────────────────────────────────────────────────────────────────────────
# API & PARSING
# ──────────────────────────────────────────────────────────────────────────────

def fetch_raw(device_id: str, lines: int = LINES, zoom: int = ZOOM) -> str:
    """Download raw text from the API. Raises on HTTP or network errors."""
    params = {"id": f"00{device_id}", "ln": lines, "zm": zoom}
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def _parse_timestamp(parts: list[str]) -> Optional[str]:
    """Build an ISO-8601 timestamp from fields 0–5 (YY;MM;DD;HH;MM;SS)."""
    try:
        dt = datetime(
            year=2000 + int(parts[0]),
            month=int(parts[1]),
            day=int(parts[2]),
            hour=int(parts[3]),
            minute=int(parts[4]),
            second=int(parts[5]),
        )
        return dt.isoformat()
    except (ValueError, IndexError):
        return None


def parse_line(line: str, field_names: list[str]) -> Optional[dict]:
    """
    Parse one semicolon-delimited CSV line.

    Returns a dict:
        {
            "timestamp": "<ISO string>",
            "raw_line":  "<original line>",
            "fields":    [{"name": str, "value": float | None}, ...],
        }
    Returns None for empty lines, API error strings, or malformed records.
    """
    line = line.strip()
    if not line or line in API_ERRORS:
        return None

    parts = [p.strip() for p in line.split(";")]

    # Need at least: 6 timestamp fields + module_id + 1 measurement value
    if len(parts) < 8:
        return None

    ts = _parse_timestamp(parts)
    if ts is None:
        return None

    # parts[6]  = module ID (skip)
    # parts[7:] = measurement values (strip trailing empty from trailing ";")
    value_parts = parts[7:]
    if value_parts and value_parts[-1] == "":
        value_parts = value_parts[:-1]

    fields: list[dict] = []
    for i, raw_val in enumerate(value_parts):
        name = field_names[i] if i < len(field_names) else f"field_{i}"
        try:
            value: Optional[float] = float(raw_val)
        except ValueError:
            value = None
        fields.append({"name": name, "value": value})

    return {"timestamp": ts, "raw_line": line, "fields": fields}

# ──────────────────────────────────────────────────────────────────────────────
# DOWNLOAD & STORE
# ──────────────────────────────────────────────────────────────────────────────

def download_device(device: dict) -> None:
    device_id = device["device_id"]
    alias = device["alias"]
    field_names: list[str] = device.get("field_names", [])

    log.info("Downloading device '%s' (id=%s) …", alias, device_id)
    rows_fetched = 0
    rows_inserted = 0

    try:
        raw = fetch_raw(device_id)

        # The API may return a single-line error message
        if raw in API_ERRORS:
            raise ValueError(f"API reported: {raw}")

        text_lines = [ln for ln in raw.splitlines() if ln.strip()]
        rows_fetched = len(text_lines)
        log.info("  Fetched %d lines", rows_fetched)

        with db_conn() as conn:
            _upsert_device(conn, device_id, alias)

            for text_line in text_lines:
                parsed = parse_line(text_line, field_names)
                if parsed is None:
                    log.debug("  Skipping unparseable line: %r", text_line)
                    continue

                try:
                    cur = conn.execute(
                        """
                        INSERT INTO measurements (device_id, timestamp, raw_line)
                        VALUES (?, ?, ?)
                        """,
                        (device_id, parsed["timestamp"], parsed["raw_line"]),
                    )
                    meas_id = cur.lastrowid
                    rows_inserted += 1
                    conn.executemany(
                        """
                        INSERT INTO measurement_fields
                            (measurement_id, field_name, field_value)
                        VALUES (?, ?, ?)
                        """,
                        [(meas_id, f["name"], f["value"]) for f in parsed["fields"]],
                    )
                except sqlite3.IntegrityError:
                    # Duplicate (device_id, timestamp) — already stored, skip silently
                    pass

            _log_run(conn, device_id, rows_fetched, rows_inserted, "ok")

        log.info(
            "  Inserted %d new rows (%d duplicates skipped)",
            rows_inserted,
            rows_fetched - rows_inserted,
        )

    except Exception as exc:
        status = f"error: {exc}"
        log.error("  Failed for device '%s': %s", alias, exc)
        try:
            with db_conn() as conn:
                _log_run(conn, device_id, rows_fetched, rows_inserted, status)
        except Exception as db_exc:
            log.error("  Could not write error to download_log: %s", db_exc)


def download_all() -> None:
    log.info("=== Download run started at %s ===", datetime.now().isoformat(timespec="seconds"))
    for device in load_devices():
        try:
            download_device(device)
        except Exception as exc:
            log.error("Unexpected error for device '%s': %s", device.get("alias"), exc)
    log.info("=== Download run finished ===")

# ──────────────────────────────────────────────────────────────────────────────
# CLI COMMANDS
# ──────────────────────────────────────────────────────────────────────────────

def cmd_status() -> None:
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT dl.id,
                   COALESCE(d.alias, dl.device_id) AS device,
                   dl.downloaded_at,
                   dl.rows_fetched,
                   dl.rows_inserted,
                   dl.status
            FROM   download_log dl
            LEFT JOIN devices d ON d.device_id = dl.device_id
            ORDER  BY dl.id DESC
            LIMIT  20
            """
        ).fetchall()

    if not rows:
        print("No download history found.")
        return

    header = f"{'ID':>5}  {'Device':<22}  {'Downloaded at':<19}  {'Fetched':>7}  {'New':>6}  Status"
    print(header)
    print("─" * len(header))
    for r in reversed(rows):
        print(
            f"{r['id']:>5}  {r['device']:<22}  {r['downloaded_at']:<19}  "
            f"{r['rows_fetched']:>7}  {r['rows_inserted']:>6}  {r['status']}"
        )


def cmd_query(alias: str) -> None:
    with db_conn() as conn:
        device = conn.execute(
            "SELECT device_id FROM devices WHERE alias = ?", (alias,)
        ).fetchone()

        if device is None:
            print(f"No device with alias '{alias}' found in the database.")
            sys.exit(1)

        rows = conn.execute(
            """
            WITH latest AS (
                SELECT id
                FROM   measurements
                WHERE  device_id = ?
                ORDER  BY timestamp DESC
                LIMIT  10
            )
            SELECT m.timestamp, mf.field_name, mf.field_value
            FROM   measurements m
            JOIN   measurement_fields mf ON mf.measurement_id = m.id
            WHERE  m.id IN (SELECT id FROM latest)
            ORDER  BY m.timestamp DESC, mf.field_name
            """,
            (device["device_id"],),
        ).fetchall()

    if not rows:
        print(f"No measurement data found for alias '{alias}'.")
        return

    print(f"Last 10 measurements for '{alias}':\n")
    current_ts: Optional[str] = None
    for r in rows:
        if r["timestamp"] != current_ts:
            current_ts = r["timestamp"]
            print(f"  {current_ts}")
        value_str = f"{r['field_value']:.4g}" if r["field_value"] is not None else "(null)"
        print(f"    {r['field_name']:<24} {value_str}")

# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="SensorFor Cloud API downloader — fetches sensor data into SQLite.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--run-now",
        action="store_true",
        help="Trigger an immediate download for all devices and exit.",
    )
    group.add_argument(
        "--status",
        action="store_true",
        help="Print the last 20 rows of download_log and exit.",
    )
    group.add_argument(
        "--query",
        metavar="DEV",
        help="Print the last 10 records for the given device alias and exit.",
    )
    args = parser.parse_args()

    init_db()

    if args.run_now:
        download_all()
        sys.exit(0)

    if args.status:
        cmd_status()
        sys.exit(0)

    if args.query:
        cmd_query(args.query)
        sys.exit(0)

    # ── Scheduled (daemon) mode ────────────────────────────────────────────────
    log.info(
        "Scheduler configured: every %s at %s. Press Ctrl+C to stop.",
        SCHEDULE_DAY,
        SCHEDULE_TIME,
    )
    getattr(schedule.every(), SCHEDULE_DAY).at(SCHEDULE_TIME).do(download_all)

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user.")


if __name__ == "__main__":
    main()
