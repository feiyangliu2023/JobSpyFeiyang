"""SQLite persistence for the job monitor.

Two tables: `jobs` (one row per unique job_url, with first/last seen timestamps
and active/gone status) and `runs` (one row per scrape run for diagnostics).
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_url       TEXT PRIMARY KEY,
    site          TEXT,
    title         TEXT,
    company       TEXT,
    location      TEXT,
    date_posted   TEXT,
    description   TEXT,
    search_name   TEXT,
    first_seen    TEXT NOT NULL,
    last_seen     TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS runs (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT NOT NULL,
    search_name  TEXT NOT NULL,
    rows_scraped INTEGER NOT NULL DEFAULT 0,
    rows_new     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen);
CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs(status);
"""


def setup_db(path: str | Path) -> sqlite3.Connection:
    """Open the SQLite DB and ensure the schema exists. Caller closes."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def upsert_jobs(
    conn: sqlite3.Connection,
    rows: Iterable[dict],
    run_started_at: str,
    search_name: str,
) -> tuple[int, int]:
    """Insert new jobs / refresh last_seen on existing ones.

    Returns (rows_scraped, rows_new). A row is "new" if its first_seen equals
    the supplied run timestamp — that's how compute_diff finds net-new jobs.
    """
    scraped = 0
    new = 0
    with transaction(conn):
        for row in rows:
            scraped += 1
            url = row.get("job_url")
            if not url:
                continue
            existing = conn.execute(
                "SELECT job_url FROM jobs WHERE job_url = ?", (url,)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE jobs
                       SET last_seen   = ?,
                           status      = 'active',
                           title       = COALESCE(?, title),
                           company     = COALESCE(?, company),
                           location    = COALESCE(?, location),
                           date_posted = COALESCE(?, date_posted),
                           description = COALESCE(?, description),
                           search_name = COALESCE(?, search_name)
                     WHERE job_url = ?
                    """,
                    (
                        run_started_at,
                        row.get("title"),
                        row.get("company"),
                        row.get("location"),
                        row.get("date_posted"),
                        row.get("description"),
                        search_name,
                        url,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO jobs
                        (job_url, site, title, company, location, date_posted,
                         description, search_name, first_seen, last_seen, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
                    """,
                    (
                        url,
                        row.get("site"),
                        row.get("title"),
                        row.get("company"),
                        row.get("location"),
                        row.get("date_posted"),
                        row.get("description"),
                        search_name,
                        run_started_at,
                        run_started_at,
                    ),
                )
                new += 1
    return scraped, new


def mark_gone(conn: sqlite3.Connection, run_started_at: str) -> int:
    """Flip rows we didn't see this run from active to gone. Returns count."""
    with transaction(conn):
        cur = conn.execute(
            """
            UPDATE jobs
               SET status = 'gone'
             WHERE status = 'active'
               AND last_seen < ?
            """,
            (run_started_at,),
        )
        return cur.rowcount


def record_run(
    conn: sqlite3.Connection,
    started_at: str,
    search_name: str,
    rows_scraped: int,
    rows_new: int,
) -> None:
    with transaction(conn):
        conn.execute(
            """
            INSERT INTO runs (started_at, search_name, rows_scraped, rows_new)
            VALUES (?, ?, ?, ?)
            """,
            (started_at, search_name, rows_scraped, rows_new),
        )


def fetch_new_since(conn: sqlite3.Connection, run_started_at: str) -> list[dict]:
    """Return jobs whose first_seen == this run's timestamp (i.e. net-new)."""
    rows = conn.execute(
        """
        SELECT job_url, site, title, company, location, date_posted, search_name
          FROM jobs
         WHERE first_seen = ?
           AND status = 'active'
         ORDER BY company, title
        """,
        (run_started_at,),
    ).fetchall()
    return [dict(r) for r in rows]
