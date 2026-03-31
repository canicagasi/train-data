"""Shared analysis functions for IC27 train delay data."""

from datetime import datetime

import duckdb
import numpy as np


def get_delay_data(db_path: str, train_num: int, station: str) -> list[tuple]:
    """Query arrival delay data from DuckDB."""
    con = duckdb.connect(db_path, read_only=True)

    tables = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables"
    ).fetchall()

    schema, table = None, None
    for s, t in tables:
        if "train_arrival" in t.lower():
            schema, table = s, t
            break

    if not table:
        con.close()
        return []

    fqn = f'"{schema}"."{table}"'

    rows = con.execute(f"""
        SELECT
            departure_date,
            scheduled_time,
            actual_time,
            difference_in_minutes
        FROM {fqn}
        WHERE train_number = ?
          AND station_short_code = ?
          AND row_type = 'ARRIVAL'
          AND actual_time IS NOT NULL
          AND cancelled = false
          AND row_cancelled = false
        ORDER BY departure_date
    """, [train_num, station]).fetchall()

    con.close()
    return rows


def analyze_delays(rows: list[tuple], deadline_min: int) -> dict | None:
    """Compute delay statistics from query results."""
    if not rows:
        return None

    dates = []
    diff_minutes = []

    for r in rows:
        dates.append(r[0])
        if r[3] is not None:
            diff_minutes.append(float(r[3]))
        elif r[1] and r[2]:
            sched = r[1] if isinstance(r[1], datetime) else datetime.fromisoformat(
                str(r[1]).replace("Z", "+00:00"))
            actual = r[2] if isinstance(r[2], datetime) else datetime.fromisoformat(
                str(r[2]).replace("Z", "+00:00"))
            diff_minutes.append((actual - sched).total_seconds() / 60.0)

    delays = np.array(diff_minutes)

    return {
        "dates": dates,
        "delays": delays,
        "count": len(delays),
        "mean": float(np.mean(delays)),
        "median": float(np.median(delays)),
        "pct_within_deadline": float(np.mean(delays <= deadline_min) * 100),
    }
