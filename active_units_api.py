#!/usr/bin/env python3
"""Active Units & Talkgroups API for Rdio-Scanner.

- Reads /home/lynx/Desktop/rdio-scanner.db
- Uses rdioScannerCalls + rdioScannerUnits / rdioScannerTalkgroups (+ rdioScannerSystems) to expose:
    * unit / talkgroup label
    * system (agency) name
    * times seen in last hour
    * last seen time
- Only includes rows with a non-empty label
- Considers only calls from the last hour
"""

import sqlite3
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify
from flask_cors import CORS

DB_PATH = "/home/lynx/Desktop/rdio-scanner.db"
MAX_AGE = timedelta(hours=1)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})


def get_db() -> sqlite3.Connection:
  """Open a connection to the rdio-scanner database."""
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  return conn


def get_system_name_map(conn: sqlite3.Connection) -> dict:
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
  except Exception as e:  # defensive
    print("Failed to build system name map:", repr(e))
    return {}


def _to_iso(raw_dt) -> str | None:
  """Convert a SQLite dateTime string to ISO 8601 with UTC if possible."""
  if raw_dt is None:
    return None

  if isinstance(raw_dt, datetime):
    dt = raw_dt
  else:
    s = str(raw_dt)
    dt = None
    # Try a couple of common formats
    for fmt in (
      "%Y-%m-%d %H:%M:%S",
      "%Y-%m-%dT%H:%M:%S",
      "%Y-%m-%d %H:%M:%S.%f",
      "%Y-%m-%dT%H:%M:%S.%f",
    ):
      try:
        dt = datetime.strptime(s, fmt)
        break
      except Exception:
        continue
    if dt is None:
      try:
        # Last resort – let fromisoformat try
        dt = datetime.fromisoformat(s)
      except Exception:
        return s

  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)

  return dt.isoformat()


@app.route("/api/active-units", methods=["GET"])
def active_units():
  """Return active labelled units from the last hour."""
  now = datetime.utcnow()
  cutoff = now - MAX_AGE
  cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

  conn = None
  try:
    conn = get_db()
    system_names = get_system_name_map(conn)

    # Group by unit label so each labelled unit appears once.
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

    results: list[dict] = []
    for row in rows:
      unit_id = row["unit_id"]
      raw_last_seen = row["last_seen_dt"]
      system_id = row["system_id"]
      seen_count = row["seen_count"]

      last_seen_iso = _to_iso(raw_last_seen)
      label = row["unit_label"]
      system_name = system_names.get(system_id, f"System {system_id}")

      results.append(
        {
          "unit_id": unit_id,
          "label": label,
          "system_id": system_id,
          "system_name": system_name,
          "seen_count": seen_count,
          "last_seen_iso": last_seen_iso,
          # keep 'agency' key for older frontends
          "agency": system_name,
        }
      )

    return jsonify(results)

  except Exception as e:
    print("Error querying active units:", repr(e))
    return jsonify({"error": "internal_error"}), 500
  finally:
    if conn is not None:
      conn.close()


@app.route("/api/active-talkgroups", methods=["GET"])
def active_talkgroups():
  """Return active labelled talkgroups from the last hour."""
  now = datetime.utcnow()
  cutoff = now - MAX_AGE
  cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

  conn = None
  try:
    conn = get_db()
    system_names = get_system_name_map(conn)

    # Group by talkgroup label so each labelled talkgroup appears once.
    sql = '''
      SELECT
        MIN(c.talkgroup)   AS talkgroup,
        MAX(c.dateTime)    AS last_seen_dt,
        MIN(c.system)      AS system_id,
        tg.label           AS tg_label,
        COUNT(*)           AS seen_count
      FROM rdioScannerCalls c
      JOIN rdioScannerTalkgroups tg
        ON tg.systemId = c.system
       AND tg.id       = c.talkgroup
      WHERE c.dateTime >= ?
        AND tg.label IS NOT NULL
        AND TRIM(tg.label) != ''
      GROUP BY tg.label
      ORDER BY last_seen_dt DESC
      LIMIT 200;
    '''
    cur = conn.execute(sql, (cutoff_str,))
    rows = cur.fetchall()

    results: list[dict] = []
    for row in rows:
      talkgroup = row["talkgroup"]
      raw_last_seen = row["last_seen_dt"]
      system_id = row["system_id"]
      seen_count = row["seen_count"]

      last_seen_iso = _to_iso(raw_last_seen)
      label = row["tg_label"]
      system_name = system_names.get(system_id, f"System {system_id}")

      results.append(
        {
          "talkgroup": talkgroup,
          "label": label,
          "system_id": system_id,
          "system_name": system_name,
          "seen_count": seen_count,
          "last_seen_iso": last_seen_iso,
          # keep 'agency' key for frontends that expect it
          "agency": system_name,
        }
      )

    return jsonify(results)

  except Exception as e:
    print("Error querying active talkgroups:", repr(e))
    return jsonify({"error": "internal_error"}), 500
  finally:
    if conn is not None:
      conn.close()


if __name__ == "__main__":
  import os

  port = int(os.environ.get("PORT", "5001"))
  app.run(host="0.0.0.0", port=port)
