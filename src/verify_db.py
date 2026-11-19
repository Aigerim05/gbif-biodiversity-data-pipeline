

import argparse
import sqlite3
from pathlib import Path


QUERIES = [
    "SELECT COUNT(*) FROM occurrences",
    "SELECT COUNT(DISTINCT key) FROM occurrences",
    "SELECT COUNT(event_date) FROM occurrences",
    "SELECT basis_of_record, COUNT(*) FROM occurrences GROUP BY basis_of_record",
    "SELECT period, COUNT(*) FROM occurrences GROUP BY period ORDER BY period",
]


def main():
    parser = argparse.ArgumentParser(description="Verify GBIF SQLite database")
    parser.add_argument("--database", default="data/output_v2.db")
    args = parser.parse_args()
    db = Path(args.database)
    if not db.is_file():
        raise SystemExit(f"Database not found: {db}. Run main.py first.")

    with sqlite3.connect(db) as conn:
        for sql in QUERIES:
            print(f"\nSQL: {sql}")
            for row in conn.execute(sql):
                print(row)


if __name__ == "__main__":
    main()
