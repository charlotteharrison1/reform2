"""Load councillor seed data from CSV into PostgreSQL."""

import csv
from pathlib import Path

import psycopg2

from config import get_db_connection


CSV_PATH = Path("seed_data/reform_councillors.csv")


def load_councillors() -> int:
    """Read the CSV file and insert councillor rows into the database."""

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    inserted = 0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Read CSV rows and insert them into the councillors table.
            with CSV_PATH.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    name = (row.get("name") or "").strip()
                    council = (row.get("council") or "").strip()
                    profile_url = (row.get("profile_url") or "").strip() or None

                    if not name or not council:
                        # Skip incomplete rows to avoid partial data inserts.
                        continue

                    cur.execute(
                        """
                        INSERT INTO councillors (name, council, profile_url)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (profile_url) DO NOTHING
                        """,
                        (name, council, profile_url),
                    )
                    if cur.rowcount == 1:
                        inserted += 1

    return inserted


if __name__ == "__main__":
    count = load_councillors()
    print(f"Inserted {count} councillor rows.")
