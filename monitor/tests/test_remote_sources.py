"""Tests for the Remotive and RemoteOK external scrapers.

Tests focus on the schema-mapping (`to_rows`) and region classifier
logic — the HTTP fetch path is exercised by the existing http_get
retry tests and isn't worth re-mocking here.
"""

from __future__ import annotations

import pytest

from monitor.external import remoteok, remotive


# --------------------------------------------------------------------------- #
# Region classification — shared shape across both modules
# --------------------------------------------------------------------------- #


class TestRemotiveRegionClassifier:
    @pytest.mark.parametrize(
        "raw",
        [
            "USA Only",
            "US-only",
            "United States",
            "U.S.",
            "Anywhere in North America",
            "USA / Canada",
            "Canada",
        ],
    )
    def test_north_america(self, raw):
        assert remotive._classify_remote_location(raw) == "north_america"

    @pytest.mark.parametrize(
        "raw",
        [
            "Europe Only",
            "EMEA",
            "United Kingdom",
            "Germany",
            "Anywhere in Europe",
            "EU-only",
        ],
    )
    def test_emea(self, raw):
        assert remotive._classify_remote_location(raw) == "emea"

    @pytest.mark.parametrize(
        "raw",
        ["", "Worldwide", "Anywhere", "Asia", "LATAM only", None],
    )
    def test_other(self, raw):
        assert remotive._classify_remote_location(raw or "") == "other"


class TestRemoteokRegionClassifier:
    def test_us_remote_variants(self):
        for v in ("Remote (US)", "Remote, US", "US only"):
            assert remoteok._classify_remote_location(v) == "north_america"

    def test_eu_remote_variants(self):
        for v in ("Remote (Europe)", "Remote, EU", "UK"):
            assert remoteok._classify_remote_location(v) == "emea"

    def test_worldwide(self):
        assert remoteok._classify_remote_location("Worldwide") == "other"


# --------------------------------------------------------------------------- #
# Remotive to_rows
# --------------------------------------------------------------------------- #


class TestRemotiveToRows:
    def test_basic_mapping(self):
        listings = [
            {
                "id": 1,
                "url": "https://remotive.com/remote-jobs/dev/foo",
                "title": "Senior Backend Engineer",
                "company_name": "Stripe",
                "category": "Software Development",
                "candidate_required_location": "USA Only",
                "publication_date": "2026-05-01T12:00:00",
                "salary": "$120k - $180k",
            }
        ]
        rows = remotive.to_rows(listings)
        assert len(rows) == 1
        r = rows[0]
        assert r["title"] == "Senior Backend Engineer"
        assert r["company"] == "Stripe"
        assert r["region"] == "north_america"
        assert r["is_remote"] is True
        assert r["site"] == "remotive"
        assert r["date_posted"] == "2026-05-01"
        # Location string is decorated with "Remote · " prefix so the
        # downstream remote-marker regex always matches.
        assert "Remote" in r["location"]
        # Salary range parsed from free text.
        assert r["min_amount"] == 120_000
        assert r["max_amount"] == 180_000
        assert r["currency"] == "USD"

    def test_skips_missing_url(self):
        listings = [
            {"title": "No URL Role", "company_name": "X", "url": ""},
            {"title": "Good Role", "company_name": "Y",
             "url": "https://remotive.com/x"},
        ]
        rows = remotive.to_rows(listings)
        assert len(rows) == 1
        assert rows[0]["title"] == "Good Role"

    def test_allowed_regions_drops_off_region(self):
        listings = [
            {"url": "https://r.com/a", "title": "x", "company_name": "x",
             "candidate_required_location": "Worldwide"},
            {"url": "https://r.com/b", "title": "y", "company_name": "y",
             "candidate_required_location": "USA Only"},
        ]
        rows = remotive.to_rows(
            listings, allowed_regions={"north_america", "emea"}
        )
        assert len(rows) == 1
        assert rows[0]["region"] == "north_america"

    def test_salary_parsing_handles_bare_numbers(self):
        # "100000" → ($100k, None)
        assert remotive._parse_salary("100000") == (100_000, None)
        # "$60k - $90k" → (60k, 90k)
        assert remotive._parse_salary("$60k - $90k") == (60_000, 90_000)
        # Empty / non-string → no salary.
        assert remotive._parse_salary("") == (None, None)
        assert remotive._parse_salary(None) == (None, None)


# --------------------------------------------------------------------------- #
# RemoteOK to_rows
# --------------------------------------------------------------------------- #


class TestRemoteokToRows:
    def test_basic_mapping(self):
        listings = [
            {
                "id": "abc",
                "url": "https://remoteok.com/remote-jobs/abc",
                "position": "Software Engineer",
                "company": "Vercel",
                "tags": ["python", "remote", "node"],
                "location": "Remote (US)",
                "date": "2026-05-01T12:00:00+00:00",
                "salary_min": 100000,
                "salary_max": 150000,
            }
        ]
        rows = remoteok.to_rows(listings)
        assert len(rows) == 1
        r = rows[0]
        assert r["title"] == "Software Engineer"
        assert r["company"] == "Vercel"
        assert r["region"] == "north_america"
        assert r["is_remote"] is True
        assert r["site"] == "remoteok"
        assert r["date_posted"] == "2026-05-01"
        assert r["min_amount"] == 100_000
        assert r["max_amount"] == 150_000
        assert r["currency"] == "USD"
        # Tags become source_category for downstream debugging.
        assert "python" in (r["source_category"] or "")

    def test_skips_missing_url(self):
        listings = [
            {"position": "x", "company": "y"},  # no url, no id
            {"position": "good", "company": "ok",
             "url": "https://r.com/a"},
        ]
        rows = remoteok.to_rows(listings)
        assert len(rows) == 1
        assert rows[0]["title"] == "good"

    def test_epoch_fallback_for_date(self):
        listings = [
            {
                "url": "https://r.com/a", "position": "x", "company": "x",
                "location": "Worldwide",
                "epoch": 1714579200,  # 2024-05-01 12:00 UTC
            },
        ]
        rows = remoteok.to_rows(listings)
        assert rows[0]["date_posted"] == "2024-05-01"

    def test_fetch_drops_legal_notice(self):
        """The first element of RemoteOK's array is always a legal
        notice with no `id` / `url`. Our fetch_listings strips it,
        but to_rows should also be robust to it landing through.
        """
        listings = [
            {"legal": "By using this API ..."},  # no id / url
            {"url": "https://r.com/a", "position": "x", "company": "x",
             "location": "Worldwide"},
        ]
        rows = remoteok.to_rows(listings)
        assert len(rows) == 1
