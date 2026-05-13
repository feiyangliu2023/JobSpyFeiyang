"""Tests for monitor/db.py — signature, upsert, prune."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from monitor import db as dbmod


def _conn():
    """Fresh in-memory DB per test."""
    return dbmod.setup_db(":memory:")


def _sig(company: str, title: str, location: str, region: str = "emea") -> str:
    return dbmod.compute_signature(
        {"company": company, "title": title, "location": location, "region": region}
    )


# --------------------------------------------------------------------------- #
# compute_signature edge cases — locked in by past commit messages
# --------------------------------------------------------------------------- #


class TestComputeSignature:
    def test_seniority_stripped_junior(self):
        """'Junior Software Engineer' must collapse with 'Software Engineer'."""
        a = _sig("Apple", "Junior Software Engineer", "London, UK")
        b = _sig("Apple", "Software Engineer", "London, UK")
        assert a == b

    def test_seniority_stripped_new_grad(self):
        """'Software Engineer, New Grad' must collapse with bare 'Software Engineer'."""
        a = _sig("Stripe", "Software Engineer, New Grad", "Dublin, IE")
        b = _sig("Stripe", "Software Engineer", "Dublin, IE")
        assert a == b

    def test_corp_suffix_stripped(self):
        """Corporate suffixes must not split a single company across signatures."""
        a = _sig("Apple", "Software Engineer", "London, UK")
        b = _sig("Apple Inc.", "Software Engineer", "London, UK")
        c = _sig("Apple Inc", "Software Engineer", "London, UK")
        assert a == b == c

    def test_cambridge_uk_vs_ma_differ(self):
        """Cambridge UK and Cambridge MA are different roles — must NOT collide.

        First-city normalisation alone collapses both to 'cambridge'; the
        `region` component in the signature carries the suffix-biased
        location classification result that distinguishes them.
        """
        uk = _sig("Apple", "Software Engineer", "Cambridge, UK", region="emea")
        ma = _sig(
            "Apple", "Software Engineer", "Cambridge, MA",
            region="north_america",
        )
        assert uk != ma

    def test_parenthetical_suffix_stripped(self):
        """Title parentheticals like '(All Genders)' must not produce a new signature."""
        a = _sig("BMW", "Software Engineer (All Genders)", "Munich, DE")
        b = _sig("BMW", "Software Engineer", "Munich, DE")
        assert a == b

    def test_year_token_stripped(self):
        """A 2026 year token in the title must not split the signature."""
        a = _sig("Google", "Software Engineer 2026", "Zurich, CH")
        b = _sig("Google", "Software Engineer", "Zurich, CH")
        assert a == b

    def test_cross_source_emea_london_collapses(self):
        """Same EMEA London role from Indeed and SimplifyJobs must share a signature.

        Indeed emits 'London, ENG, GB'; SimplifyJobs emits 'London, UK'.
        The first-city normaliser collapses both to 'london'; `region`
        matches because the location classifier hands both 'emea'.
        """
        indeed = _sig("Apple", "Software Engineer", "London, ENG, GB")
        simplify = _sig("Apple", "Software Engineer", "London, UK")
        assert indeed == simplify


# --------------------------------------------------------------------------- #
# upsert_jobs — insert-new + refresh-existing
# --------------------------------------------------------------------------- #


def _row(job_url: str, **kwargs) -> dict:
    base = {
        "job_url": job_url,
        "site": "indeed",
        "title": "Software Engineer",
        "company": "Apple",
        "location": "London, UK",
        "region": "emea",
    }
    base.update(kwargs)
    return base


class TestUpsertJobs:
    def test_insert_new_returns_new_count(self):
        conn = _conn()
        ts = "2026-05-14T10:00:00+00:00"
        scraped, new = dbmod.upsert_jobs(
            conn, [_row("https://a"), _row("https://b")], ts, "test_search"
        )
        assert scraped == 2
        assert new == 2

    def test_refresh_existing_does_not_count_as_new(self):
        conn = _conn()
        ts1 = "2026-05-14T10:00:00+00:00"
        ts2 = "2026-05-14T22:00:00+00:00"
        dbmod.upsert_jobs(conn, [_row("https://a")], ts1, "search_1")
        scraped, new = dbmod.upsert_jobs(
            conn, [_row("https://a")], ts2, "search_2"
        )
        assert scraped == 1
        assert new == 0
        # last_seen advanced; first_seen pinned to original insert time.
        r = conn.execute(
            "SELECT first_seen, last_seen FROM jobs WHERE job_url=?",
            ("https://a",),
        ).fetchone()
        assert r["first_seen"] == ts1
        assert r["last_seen"] == ts2

    def test_coalesce_preserves_existing_description(self):
        """Existing non-null value wins when the incoming row has null."""
        conn = _conn()
        ts1 = "2026-05-14T10:00:00+00:00"
        ts2 = "2026-05-14T22:00:00+00:00"
        dbmod.upsert_jobs(
            conn,
            [_row("https://a", description="Original description")],
            ts1, "search_1",
        )
        dbmod.upsert_jobs(
            conn,
            [_row("https://a", description=None)],
            ts2, "search_2",
        )
        r = conn.execute(
            "SELECT description FROM jobs WHERE job_url=?", ("https://a",)
        ).fetchone()
        assert r["description"] == "Original description"

    def test_skips_rows_with_no_url(self):
        conn = _conn()
        ts = "2026-05-14T10:00:00+00:00"
        scraped, new = dbmod.upsert_jobs(
            conn, [_row("https://a"), {"company": "Apple"}], ts, "test"
        )
        assert scraped == 2
        assert new == 1  # the malformed row was counted as scraped but not inserted

    def test_signature_written_on_insert(self):
        conn = _conn()
        ts = "2026-05-14T10:00:00+00:00"
        dbmod.upsert_jobs(conn, [_row("https://a")], ts, "test")
        r = conn.execute(
            "SELECT signature FROM jobs WHERE job_url=?", ("https://a",)
        ).fetchone()
        assert r["signature"] == _sig("Apple", "Software Engineer", "London, UK")


# --------------------------------------------------------------------------- #
# prune_old — 180-day retention boundary
# --------------------------------------------------------------------------- #


class TestPruneOld:
    def test_prunes_old_gone_rows(self):
        conn = _conn()
        now = datetime.now(timezone.utc)
        old = (now - timedelta(days=200)).isoformat(timespec="seconds")
        fresh = (now - timedelta(days=10)).isoformat(timespec="seconds")
        # Old gone — should prune
        conn.execute(
            "INSERT INTO jobs(job_url, first_seen, last_seen, status, region) "
            "VALUES (?, ?, ?, 'gone', 'emea')",
            ("https://old-gone", old, old),
        )
        # Recent gone — should keep
        conn.execute(
            "INSERT INTO jobs(job_url, first_seen, last_seen, status, region) "
            "VALUES (?, ?, ?, 'gone', 'emea')",
            ("https://recent-gone", fresh, fresh),
        )
        # Old active — should keep (only gone rows get pruned)
        conn.execute(
            "INSERT INTO jobs(job_url, first_seen, last_seen, status, region) "
            "VALUES (?, ?, ?, 'active', 'emea')",
            ("https://old-active", old, old),
        )
        conn.commit()

        stats = dbmod.prune_old(conn, retention_days=180)
        assert stats["jobs_pruned"] == 1

        urls = {
            r["job_url"]
            for r in conn.execute("SELECT job_url FROM jobs").fetchall()
        }
        assert urls == {"https://recent-gone", "https://old-active"}

    def test_prunes_old_runs_at_half_window(self):
        """Runs are kept for retention_days // 2, not the full window."""
        conn = _conn()
        now = datetime.now(timezone.utc)
        # 100 days old — within jobs window (180d) but past runs window (90d)
        old_run = (now - timedelta(days=100)).isoformat(timespec="seconds")
        fresh_run = (now - timedelta(days=30)).isoformat(timespec="seconds")
        conn.execute(
            "INSERT INTO runs(started_at, search_name, rows_scraped, rows_new) "
            "VALUES (?, ?, 0, 0)",
            (old_run, "old"),
        )
        conn.execute(
            "INSERT INTO runs(started_at, search_name, rows_scraped, rows_new) "
            "VALUES (?, ?, 0, 0)",
            (fresh_run, "fresh"),
        )
        conn.commit()

        stats = dbmod.prune_old(conn, retention_days=180)
        assert stats["runs_pruned"] == 1
        remaining = [
            r["search_name"]
            for r in conn.execute("SELECT search_name FROM runs").fetchall()
        ]
        assert remaining == ["fresh"]

    def test_180_day_boundary_keeps_just_inside(self):
        """A row aged 179 days survives; 181 days does not."""
        conn = _conn()
        now = datetime.now(timezone.utc)
        inside = (now - timedelta(days=179)).isoformat(timespec="seconds")
        outside = (now - timedelta(days=181)).isoformat(timespec="seconds")
        for url, ts in (("https://inside", inside), ("https://outside", outside)):
            conn.execute(
                "INSERT INTO jobs(job_url, first_seen, last_seen, status, region) "
                "VALUES (?, ?, ?, 'gone', 'emea')",
                (url, ts, ts),
            )
        conn.commit()

        dbmod.prune_old(conn, retention_days=180)
        urls = {
            r["job_url"]
            for r in conn.execute("SELECT job_url FROM jobs").fetchall()
        }
        assert urls == {"https://inside"}

    def test_no_op_when_nothing_to_prune(self):
        conn = _conn()
        stats = dbmod.prune_old(conn, retention_days=180)
        assert stats == {"jobs_pruned": 0, "runs_pruned": 0}

    def test_vacuum_shrinks_file(self, tmp_path):
        """End-to-end: write 100 gone rows, prune, file size drops noticeably."""
        db_path = tmp_path / "shrink.db"
        conn = dbmod.setup_db(db_path)
        now = datetime.now(timezone.utc)
        old = (now - timedelta(days=365)).isoformat(timespec="seconds")
        big_desc = "x" * 4096  # 4 KB per row
        for i in range(100):
            conn.execute(
                "INSERT INTO jobs(job_url, first_seen, last_seen, status, "
                "region, description) "
                "VALUES (?, ?, ?, 'gone', 'emea', ?)",
                (f"https://row{i}", old, old, big_desc),
            )
        conn.commit()
        conn.close()

        size_before = os.path.getsize(db_path)

        conn = dbmod.setup_db(db_path)
        stats = dbmod.prune_old(conn, retention_days=180)
        conn.close()

        size_after = os.path.getsize(db_path)
        assert stats["jobs_pruned"] == 100
        # Should be at least 100KB smaller — 100 rows × ~4KB each. Loose
        # threshold so a filesystem rounding the page size doesn't flake
        # the test on Windows / Linux.
        assert size_before - size_after > 100_000, (
            f"VACUUM didn't shrink the file meaningfully: "
            f"{size_before} → {size_after} bytes"
        )


# --------------------------------------------------------------------------- #
# fetch_new_since signature-aware dedup
# --------------------------------------------------------------------------- #


class TestFetchNewSinceDedup:
    def test_prefers_direct_over_simplify(self):
        """direct:* always outranks SimplifyJobs for the same signature."""
        conn = _conn()
        ts = "2026-05-14T10:00:00+00:00"
        # Same role surfaced by two sources in the same run
        dbmod.upsert_jobs(
            conn,
            [
                _row(
                    "https://simplify/role",
                    site="simplify_newgrad",
                    company="Anthropic",
                    title="Software Engineer",
                    location="London, UK",
                ),
                _row(
                    "https://anthropic/role",
                    site="direct:anthropic",
                    company="Anthropic",
                    title="Software Engineer",
                    location="London, UK",
                ),
            ],
            ts, "test_search",
        )
        new = dbmod.fetch_new_since(conn, ts)
        assert len(new) == 1
        assert new[0]["site"] == "direct:anthropic"

    def test_prefers_simplify_over_indeed(self):
        conn = _conn()
        ts = "2026-05-14T10:00:00+00:00"
        dbmod.upsert_jobs(
            conn,
            [
                _row(
                    "https://indeed/role",
                    site="indeed",
                    company="Stripe",
                    title="Software Engineer",
                    location="Dublin, IE",
                ),
                _row(
                    "https://simplify/role",
                    site="simplify_newgrad",
                    company="Stripe",
                    title="Software Engineer",
                    location="Dublin, IE",
                ),
            ],
            ts, "test_search",
        )
        new = dbmod.fetch_new_since(conn, ts)
        assert len(new) == 1
        assert new[0]["site"] == "simplify_newgrad"

    def test_rows_without_signature_pass_through(self):
        """A row with NULL signature shouldn't drop out — render handles dedup elsewhere."""
        conn = _conn()
        ts = "2026-05-14T10:00:00+00:00"
        dbmod.upsert_jobs(conn, [_row("https://a")], ts, "test")
        # Manually NULL the signature
        conn.execute("UPDATE jobs SET signature = NULL WHERE job_url=?", ("https://a",))
        conn.commit()
        new = dbmod.fetch_new_since(conn, ts)
        assert len(new) == 1
