

import math
import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd

DDL = """CREATE TABLE IF NOT EXISTS occurrences (
    key INTEGER PRIMARY KEY,
    scientific_name TEXT NOT NULL,
    kingdom TEXT NOT NULL,
    basis_of_record TEXT NOT NULL,
    event_date TEXT,
    year INTEGER,
    decimal_latitude REAL NOT NULL CHECK(decimal_latitude BETWEEN -90 AND 90),
    decimal_longitude REAL NOT NULL CHECK(decimal_longitude BETWEEN -180 AND 180),
    country_code TEXT NOT NULL CHECK(country_code = 'KZ'),
    dataset_key TEXT,
    period TEXT,
    distance_to_almaty_km REAL NOT NULL CHECK(distance_to_almaty_km >= 0)
)"""

COLUMNS = (
    "key", "scientific_name", "kingdom", "basis_of_record", "event_date", "year",
    "decimal_latitude", "decimal_longitude", "country_code", "dataset_key",
    "period", "distance_to_almaty_km",
)
INSERT = (
    "INSERT INTO occurrences (" + ", ".join(COLUMNS) + ") VALUES (" +
    ", ".join("?" for _ in COLUMNS) + ")"
)
FLOAT_COLUMNS = {"decimal_latitude", "decimal_longitude", "distance_to_almaty_km"}


class RecordConflictError(ValueError):
    """An existing GBIF ID has different data than the incoming record."""


def to_rows(frame):
    """Convert cleaned Pandas rows to SQLite-compatible tuples."""
    for _, row in frame.iterrows():
        event = row["eventDate"]
        yield (
            int(row["key"]), str(row["scientificName"]), str(row["kingdom"]),
            str(row["basisOfRecord"]),
            None if pd.isna(event) else event.isoformat(),
            None if pd.isna(row["year"]) else int(row["year"]),
            float(row["decimalLatitude"]), float(row["decimalLongitude"]),
            str(row["countryCode"]),
            None if pd.isna(row["datasetKey"]) else str(row["datasetKey"]),
            None if pd.isna(row["period"]) else str(row["period"]),
            float(row["distance_to_almaty_km"]),
        )


def matching_record(expected, stored):
    """Compare all columns; tolerate insignificant float round-off only."""
    for column, a, b in zip(COLUMNS, expected, stored):
        if column in FLOAT_COLUMNS:
            if a is None or b is None or not math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-8):
                return False
        elif a != b:
            return False
    return True


def verify_existing(conn, records):
    """Check for changed records before any insertion or database mutation."""
    query = f"SELECT {', '.join(COLUMNS)} FROM occurrences WHERE key = ?"
    for record in records:
        existing = conn.execute(query, (record[0],)).fetchone()
        if existing is not None and not matching_record(record, existing):
            changed = [col for col, incoming, old in zip(COLUMNS, record, existing)
                       if not matching_record_value(col, incoming, old)]
            raise RecordConflictError(
                f"GBIF key {record[0]} already exists with different fields: "
                f"{', '.join(changed)}. Original database was not changed. "
                "Use the original --reuse dataset or a new database for a different snapshot."
            )


def matching_record_value(column, a, b):
    if column in FLOAT_COLUMNS:
        return a is not None and b is not None and math.isclose(
            a, b, rel_tol=1e-12, abs_tol=1e-8
        )
    return a == b


def snapshot(conn, keys):
    query = f"SELECT {', '.join(COLUMNS)} FROM occurrences WHERE key = ?"
    return {key: conn.execute(query, (key,)).fetchone() for key in keys}


def load_twice(frame, database="data/output_v2.db"):
    """Insert the batch twice, verify identical stored content, return SQL data.

    Existing keys are immutable. A content conflict raises before any new row
    is inserted, so there is no partial update on a failing batch.
    """
    records = list(to_rows(frame))
    if not records:
        raise ValueError("Cannot load an empty batch")
    keys = [record[0] for record in records]
    if len(keys) != len(set(keys)):
        raise ValueError("The input batch contains duplicate GBIF keys")

    path = Path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(DDL)
        # One transaction for both inserts and both checks; no partial batch commits.
        try:
            conn.execute("BEGIN")
            verify_existing(conn, records)
            for record in records:
                if conn.execute("SELECT 1 FROM occurrences WHERE key=?", (record[0],)).fetchone() is None:
                    conn.execute(INSERT, record)
            first = conn.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
            after_first = snapshot(conn, keys)
            verify_existing(conn, records)
            # Second load: all keys already exist and must match exactly.
            for record in records:
                if conn.execute("SELECT 1 FROM occurrences WHERE key=?", (record[0],)).fetchone() is None:
                    conn.execute(INSERT, record)
            second = conn.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
            after_second = snapshot(conn, keys)
            if first != second or after_first != after_second:
                raise RuntimeError("Repeated load changed the row count or stored content")
            if any(row is None for row in after_second.values()):
                raise RuntimeError("Current batch is not fully present in SQLite")
            unique = conn.execute("SELECT COUNT(DISTINCT key) FROM occurrences").fetchone()[0]
            if second != unique:
                raise RuntimeError("SQLite unique-ID verification failed")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        # EDA is based on data read back from SQLite, scoped to this batch.
        db = pd.read_sql_query("SELECT * FROM occurrences", conn)
        batch_frame = db[db["key"].isin(keys)].copy()
        if len(batch_frame) != len(records):
            raise RuntimeError("Stored batch record count differs from processed record count")
        sql_results = {
            "count_query": "SELECT COUNT(*) AS total FROM occurrences",
            "count_result": second,
            "unique_query": "SELECT COUNT(DISTINCT key) AS unique_keys FROM occurrences",
            "unique_result": unique,
            "basis_query": "SELECT basis_of_record, COUNT(*) AS n FROM occurrences GROUP BY basis_of_record ORDER BY n DESC",
            "basis_result": [list(row) for row in conn.execute(
                "SELECT basis_of_record, COUNT(*) FROM occurrences "
                "GROUP BY basis_of_record ORDER BY COUNT(*) DESC"
            )],
        }
    results = {
        "processed_records": len(records),
        "first_load_total": first,
        "second_load_total": second,
        "unique_database_keys": unique,
        "database_total": len(db),
        "current_batch_stored": len(batch_frame),
        "database_contains_current_batch": True,
        "unchanged_on_second_load": True,
        "sql_verification": sql_results,
    }
    return batch_frame, results
