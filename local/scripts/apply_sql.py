"""Apply local/sql/*.sql in order. Safe to re-run (the files use IF NOT EXISTS).

Postgres only runs these files automatically the first time its volume is created, so use
this script for an existing database.

    python scripts/apply_sql.py
"""
from pathlib import Path

from vidpipe import db

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def main():
    files = sorted(SQL_DIR.glob("*.sql"))
    with db.connect() as conn:
        for f in files:
            conn.execute(f.read_text())
            print(f"applied {f.name}")


if __name__ == "__main__":
    main()
