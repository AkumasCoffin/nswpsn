#!/usr/bin/env python3
"""Active Units & Talkgroups API for Rdio-Scanner.

- Reads SQLite DB defined in RDIO_SCANNER_DB environment variable.
- Exposes:
    * /api/active-units
    * /api/active-talkgroups
"""

import os
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify
from flask_cors import CORS

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
# Default to desktop path if env var not set
DB_PATH = os.environ.get("RDIO_SCANNER_DB", "/home/lynx/Desktop/rdio-scanner.db")
MAX_AGE = timedelta(hours=1)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_system_name_map(conn):
    """Return mapping systemId -> human readable name."""
    try:
        cur = conn.execute("PRAGMA table_info(rdioScannerSystems)")
        cols = [row[1] for row in cur.fetchall()]

        name_col = None
        for candidate in ("name", "label", "shortName", "short_name"):
            if candidate in cols:
                name_col = candidate
                break

        if not name_col:
            return {}

        sql = f"SELECT id, {name_col} AS name FROM rdioScannerSystems"
        cur = conn.execute(sql)
        return {row["id"]: row["name"] for row in cur.fetchall()}
    except Exception:
        logging.error("Failed to build system name map.", exc_info=True)
        return {}


@app.route("/api/active-units", methods=["GET"])
def active_units():
    now = datetime.utcnow()
    cutoff = now - MAX_AGE
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    conn = None
    try:
        conn = get_db()
        system_names = get_system_name_map(conn)

        # Group by label so each labelled unit appears once.
        sql = '''
            SELECT
              MIN(c.source)      AS unit_id,
              MAX(c.dateTime)    AS last_seen_dt,
              MIN(c.system)      AS system_id,
              u.label            AS unit_label,
              COUNT(*)           AS seen_count
            FROM rdioScannerCalls c
            JOIN rdioScannerUnits u
              ON u.systemId = c.system
             AND u.id       = c.source
            WHERE c.dateTime >= ?
              AND u.label IS NOT NULL
              AND TRIM(u.label) != ''
            GROUP BY u.label
            ORDER BY last_seen_dt DESC
            LIMIT 200;
        '''
        cur = conn.execute(sql, (cutoff_str,))
        rows = cur.fetchall()

        results = []
        for row in rows:
            unit_id = row["unit_id"]
            raw_last_seen = row["last_seen_dt"]
            system_id = row["system_id"]
            seen_count = row["seen_count"]

            last_seen_iso = None
            if raw_last_seen:
                try:
                    dt = datetime.fromisoformat(raw_last_seen)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    last_seen_iso = dt.isoformat()
                except Exception:
                    last_seen_iso = str(raw_last_seen)

            label = row["unit_label"]
            system_name = system_names.get(system_id, f"System {system_id}")

            results.append({
                "unit_id": unit_id,
                "label": label,
                "system_id": system_id,
                "system_name": system_name,
                "seen_count": seen_count,
                "last_seen_iso": last_seen_iso,
                "agency": system_name,
            })

        return jsonify(results)

    except Exception:
        logging.error("Error querying active units.", exc_info=True)
        return jsonify({"error": "internal_error"}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route("/api/active-talkgroups", methods=["GET"])
def active_talkgroups():
    now = datetime.utcnow()
    cutoff = now - MAX_AGE
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    conn = None
    try:
        conn = get_db()
        system_names = get_system_name_map(conn)

        # Group by label so each labelled talkgroup appears once.
        sql = '''
            SELECT
              MIN(c.talkgroup)   AS talkgroup_id,
              MAX(c.dateTime)    AS last_seen_dt,
              MIN(c.system)      AS system_id,
              t.label            AS talkgroup_label,
              COUNT(*)           AS seen_count
            FROM rdioScannerCalls c
            JOIN rdioScannerTalkgroups t
              ON t.systemId = c.system
             AND t.id       = c.talkgroup
            WHERE c.dateTime >= ?
              AND t.label IS NOT NULL
              AND TRIM(t.label) != ''
            GROUP BY t.label
            ORDER BY last_seen_dt DESC
            LIMIT 200;
        '''
        cur = conn.execute(sql, (cutoff_str,))
        rows = cur.fetchall()

        results = []
        for row in rows:
            talkgroup_id = row["talkgroup_id"]
            raw_last_seen = row["last_seen_dt"]
            system_id = row["system_id"]
            seen_count = row["seen_count"]

            last_seen_iso = None
            if raw_last_seen:
                try:
                    dt = datetime.fromisoformat(raw_last_seen)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    last_seen_iso = dt.isoformat()
                except Exception:
                    last_seen_iso = str(raw_last_seen)

            label = row["talkgroup_label"]
            system_name = system_names.get(system_id, f"System {system_id}")

            results.append({
                "talkgroup_id": talkgroup_id,
                "label": label,
                "system_id": system_id,
                "system_name": system_name,
                "seen_count": seen_count,
                "last_seen_iso": last_seen_iso,
                "agency": system_name,
            })

        return jsonify(results)

    except Exception:
        logging.error("Error querying active talkgroups.", exc_info=True)
        return jsonify({"error": "internal_error"}), 500
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    logging.info(f"Starting API server on port {port}...")
    app.run(host="0.0.0.0", port=port)
