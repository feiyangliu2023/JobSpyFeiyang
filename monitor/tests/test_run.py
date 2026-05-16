"""Tests for monitor/run.py — search expansion + per-source region gating.

The single-country JobSpy scrapers (naukri = India, bdjobs = Bangladesh,
zip_recruiter = USA/Canada) used to be enabled for every EMEA city and
silently returned 0 rows on every call, flagging them SILENT in the
health report. `expand_searches` now drops them per-city via
`_SITE_COUNTRY_SUPPORTED`, the same pattern Glassdoor uses; these tests
lock that behaviour in.
"""

from __future__ import annotations

import os

import pytest

from monitor.run import expand_searches


@pytest.fixture
def base_cfg():
    """Minimal config exercising every source we gate per-country."""
    return {
        "cities": [
            {"name": "london", "location": "London, United Kingdom", "country_indeed": "uk"},
            {"name": "bengaluru", "location": "Bengaluru, India", "country_indeed": "india"},
            {"name": "dhaka", "location": "Dhaka, Bangladesh", "country_indeed": "bangladesh"},
            {"name": "nyc", "location": "New York, USA", "country_indeed": "usa"},
            {"name": "stockholm", "location": "Stockholm, Sweden", "country_indeed": "sweden"},
        ],
        "role_templates": [
            {
                "name": "sde_junior",
                "sites": [
                    "indeed", "linkedin", "glassdoor", "bayt",
                    "google", "naukri", "bdjobs", "zip_recruiter",
                ],
                "search_terms": ["software engineer"],
                "results_wanted": 10,
                "hours_old": 168,
            },
        ],
        "filters": {},
    }


def _sites_for_city(searches, city_name):
    return {s["site"] for s in searches if s["name"].endswith(f"_{city_name}")}


def test_naukri_only_runs_for_india_cities(monkeypatch, base_cfg):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    searches = expand_searches(base_cfg)

    # India city includes naukri; all others drop it.
    assert "naukri" in _sites_for_city(searches, "bengaluru")
    for off_region in ("london", "dhaka", "nyc", "stockholm"):
        assert "naukri" not in _sites_for_city(searches, off_region)


def test_bdjobs_only_runs_for_bangladesh_cities(monkeypatch, base_cfg):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    searches = expand_searches(base_cfg)

    assert "bdjobs" in _sites_for_city(searches, "dhaka")
    for off_region in ("london", "bengaluru", "nyc", "stockholm"):
        assert "bdjobs" not in _sites_for_city(searches, off_region)


def test_ziprecruiter_only_runs_for_us_canada_cities(monkeypatch, base_cfg):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    searches = expand_searches(base_cfg)

    assert "zip_recruiter" in _sites_for_city(searches, "nyc")
    for off_region in ("london", "bengaluru", "dhaka", "stockholm"):
        assert "zip_recruiter" not in _sites_for_city(searches, off_region)


def test_glassdoor_drops_for_no_tld_country(monkeypatch, base_cfg):
    """Sweden has no Glassdoor TLD — pre-existing behaviour, kept here as a
    regression guard since the new per-site gating is wired through the
    same code path."""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    searches = expand_searches(base_cfg)

    assert "glassdoor" in _sites_for_city(searches, "london")
    assert "glassdoor" not in _sites_for_city(searches, "stockholm")


def test_indeed_runs_for_every_city(monkeypatch, base_cfg):
    """Indeed has no country gate — it's the universal source."""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("CI", raising=False)
    searches = expand_searches(base_cfg)
    for city in ("london", "bengaluru", "dhaka", "nyc", "stockholm"):
        assert "indeed" in _sites_for_city(searches, city)


def test_ci_skip_drops_google_and_blocked_sources(monkeypatch, base_cfg):
    """In CI the config sets sites_skip_in_ci, which must run AFTER the
    country gate. We pass the full skip list to verify CI strips
    linkedin/glassdoor/bayt/google globally on top of the regional drops."""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    base_cfg["role_templates"][0]["sites_skip_in_ci"] = [
        "linkedin", "glassdoor", "bayt", "google",
    ]
    searches = expand_searches(base_cfg)

    # London (EMEA): only indeed survives CI mode.
    assert _sites_for_city(searches, "london") == {"indeed"}
    # Bengaluru still runs naukri in CI — naukri isn't in the CI-skip list.
    assert _sites_for_city(searches, "bengaluru") == {"indeed", "naukri"}


def test_country_case_and_whitespace_tolerance(base_cfg):
    """Country strings can be mixed case or whitespace-padded in user
    configs; the predicates normalise both."""
    base_cfg["cities"] = [
        {"name": "mumbai", "location": "Mumbai", "country_indeed": "  India  "},
        {"name": "toronto", "location": "Toronto", "country_indeed": "Canada"},
    ]
    searches = expand_searches(base_cfg)
    assert "naukri" in _sites_for_city(searches, "mumbai")
    assert "zip_recruiter" in _sites_for_city(searches, "toronto")
