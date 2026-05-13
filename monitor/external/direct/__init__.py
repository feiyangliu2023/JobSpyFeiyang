"""Direct-from-careers-page scrapers for tier-1 target companies.

SimplifyJobs is hand-curated (delayed by however long it takes a maintainer
to merge a PR) and JobSpy bottlenecks on LinkedIn / Indeed rate limits. For
the user's high-priority targets we scrape the careers page directly so a
new posting lands in JOBS.md within hours instead of days.

Each company gets its own thin module (`monitor/external/direct/<co>.py`)
that exposes a single `fetch_listings() -> list[dict]` function returning
rows in the same shape `monitor.external.simplify.to_rows` produces — so
they drop straight into `apply_filters` + `dbmod.upsert_jobs`.

Rows are tagged `site="direct:<company>"`, which:
  - Distinguishes them from JobSpy / SimplifyJobs entries in the DB.
  - Lets `render_md._source_rank` rank them ABOVE simplify_* during
    cross-source dedup — they have direct apply URLs and zero aggregator
    lag.

## The pattern

Most well-funded companies post via Greenhouse, Ashby, Lever, or
SmartRecruiters. Each ATS exposes a public JSON job board feed; a per-
company scraper is usually ~20 lines wrapping one of the helpers below.

  - Greenhouse  → `fetch_greenhouse(board_token)`
  - Ashby       → `fetch_ashby(board_name)`

See `README.md` for the company → ATS mapping cheat sheet.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from monitor.external import build_headers, http_get, read_cache, write_cache
from monitor.external.locations import classify_locations

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared row builder
# ---------------------------------------------------------------------------


def make_row(
    *,
    site_label: str,
    job_url: str,
    company: str,
    title: str,
    location_strings: list[str],
    company_url: str = "",
    posted_iso: str | None = None,
    is_remote: bool | None = None,
    description: str | None = None,
) -> dict:
    """Build a jobs.db row from raw scraper fields.

    `location_strings` is a list of human-readable city strings (e.g.
    ["San Francisco, CA", "New York City, NY"]). We classify region from
    them via `classify_locations`, then join the first three for the
    visible `location` column — matches `monitor.external.simplify.to_rows`.

    `posted_iso` may be any ISO 8601 timestamp; we keep only the date
    portion (`YYYY-MM-DD`) so render_md's age column works the same way
    it does for SimplifyJobs rows.
    """
    if location_strings:
        _, region = classify_locations(location_strings)
    else:
        region = "other"
    visible_location = " · ".join(
        s for s in location_strings[:3] if s
    ).strip()

    date_posted: str | None = None
    if posted_iso:
        # Greenhouse returns "2026-03-26T06:01:22-04:00"; Ashby returns
        # "2026-03-12T16:38:15.322+00:00". Both share the leading YYYY-MM-DD.
        date_posted = posted_iso[:10]
        # Guard against junk
        if not (
            len(date_posted) == 10
            and date_posted[4] == "-"
            and date_posted[7] == "-"
        ):
            date_posted = None

    return {
        "job_url": job_url,
        "site": site_label,
        "title": title or "",
        "company": company or "",
        "company_url": company_url or "",
        "location": visible_location,
        "is_remote": is_remote,
        "date_posted": date_posted,
        "description": description,
        "min_amount": None,
        "max_amount": None,
        "currency": None,
        "salary_interval": None,
        "region": region,
        "source_category": None,
    }


# ---------------------------------------------------------------------------
# Greenhouse helper
# ---------------------------------------------------------------------------


GREENHOUSE_BASE = "https://boards-api.greenhouse.io/v1/boards/{board}/jobs"


def fetch_greenhouse(board_token: str, timeout: int = 60) -> list[dict]:
    """GET the Greenhouse public job board feed. Returns [] on any error.

    `board_token` is the company's slug at greenhouse.io. The endpoint
    returns the entire active job list in one response — no pagination.

    Bounded retry + ETag / Last-Modified caching: a 304 from upstream
    means we serve the previously parsed jobs from
    `monitor/cache/greenhouse_<board>.json` so a partial CDN block
    keeps producing rows instead of going dark.
    """
    url = GREENHOUSE_BASE.format(board=board_token)
    cache_key = f"greenhouse_{board_token}"
    source_label = f"direct.greenhouse:{board_token}"
    log.info("%s: GET %s", source_label, url)
    try:
        result = http_get(
            url,
            source_label=source_label,
            headers=build_headers(
                referer=f"https://boards.greenhouse.io/{board_token}"
            ),
            timeout=timeout,
            cache_key=cache_key,
        )
    except Exception as e:
        log.exception("%s: fetch failed: %s", source_label, e)
        return []

    if result.cached:
        cache = read_cache(cache_key) or {}
        jobs = cache.get("jobs") or []
        log.info(
            "%s: 304 not modified — %d jobs served from cache",
            source_label, len(jobs),
        )
        return jobs if isinstance(jobs, list) else []

    try:
        data = json.loads(result.body or b"")
    except Exception as e:
        log.exception("%s: JSON parse failed: %s", source_label, e)
        return []
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        log.warning(
            "%s: unexpected payload shape: %r",
            source_label, type(data).__name__,
        )
        return []

    # Persist parsed jobs alongside the new validators so a future 304
    # response can be resolved from local cache.
    if result.etag or result.last_modified:
        write_cache(cache_key, {
            "etag": result.etag,
            "last_modified": result.last_modified,
            "jobs": jobs,
        })
    log.info("%s: %d jobs", source_label, len(jobs))
    return jobs


def _split_greenhouse_location(loc_name: str) -> list[str]:
    """Greenhouse multi-location strings are pipe-separated.

    "San Francisco, CA | New York City, NY" → ["San Francisco, CA",
    "New York City, NY"].
    """
    if not loc_name:
        return []
    return [p.strip() for p in loc_name.split("|") if p.strip()]


def greenhouse_to_rows(
    jobs: list[dict],
    *,
    site_label: str,
    company: str,
    company_url: str = "",
) -> list[dict]:
    """Map Greenhouse jobs[] payload → jobs.db row shape.

    Greenhouse job schema (`/v1/boards/{token}/jobs`, no content):
      id, title, location {name}, absolute_url, first_published,
      updated_at, company_name, internal_job_id, requisition_id,
      data_compliance[], language, metadata[]
    """
    rows: list[dict] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        url = (j.get("absolute_url") or "").strip()
        if not url:
            continue
        loc_name = (j.get("location") or {}).get("name") or ""
        rows.append(
            make_row(
                site_label=site_label,
                job_url=url,
                company=j.get("company_name") or company,
                title=j.get("title") or "",
                location_strings=_split_greenhouse_location(loc_name),
                company_url=company_url,
                posted_iso=j.get("first_published") or j.get("updated_at"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Ashby helper
# ---------------------------------------------------------------------------


ASHBY_BASE = "https://api.ashbyhq.com/posting-api/job-board/{board}"


def fetch_ashby(board_name: str, timeout: int = 60) -> list[dict]:
    """GET the Ashby public posting-api job board feed. [] on error.

    Note: the URL is bare `/job-board/{board}` — the `/jobs` suffix that
    some docs reference is for the *authenticated* API and 401's here.

    Bounded retry + ETag / Last-Modified caching: a 304 from upstream
    means we serve the previously parsed jobs from
    `monitor/cache/ashby_<board>.json` so a partial CDN block keeps
    producing rows instead of going dark.
    """
    url = ASHBY_BASE.format(board=board_name)
    cache_key = f"ashby_{board_name}"
    source_label = f"direct.ashby:{board_name}"
    log.info("%s: GET %s", source_label, url)
    try:
        result = http_get(
            url,
            source_label=source_label,
            headers=build_headers(
                referer=f"https://jobs.ashbyhq.com/{board_name}"
            ),
            timeout=timeout,
            cache_key=cache_key,
        )
    except Exception as e:
        log.exception("%s: fetch failed: %s", source_label, e)
        return []

    if result.cached:
        cache = read_cache(cache_key) or {}
        jobs = cache.get("jobs") or []
        log.info(
            "%s: 304 not modified — %d jobs served from cache",
            source_label, len(jobs),
        )
        return jobs if isinstance(jobs, list) else []

    try:
        data = json.loads(result.body or b"")
    except Exception as e:
        log.exception("%s: JSON parse failed: %s", source_label, e)
        return []
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        log.warning(
            "%s: unexpected payload shape: %r",
            source_label, type(data).__name__,
        )
        return []

    if result.etag or result.last_modified:
        write_cache(cache_key, {
            "etag": result.etag,
            "last_modified": result.last_modified,
            "jobs": jobs,
        })
    log.info("%s: %d jobs", source_label, len(jobs))
    return jobs


def _ashby_enrich_location(loc: str, address: dict | None) -> str:
    """Append `, <country>` to a bare-city Ashby location string.

    Ashby's `location` is often just a city ("San Francisco") with no
    country suffix — the location classifier in monitor.external.locations
    is suffix-biased, so a bare US city falls to "other" instead of
    "north_america". Ashby gives us `address.postalAddress.addressCountry`
    separately ("United States" / "United Kingdom" / etc), which the
    classifier knows; gluing it on fixes the classification AND makes the
    rendered location more useful.

    Strings already containing a comma are returned untouched — the
    intent is to enrich BARE city strings only, not second-guess
    well-formed "City, Country" labels.
    """
    if not loc:
        return ""
    if "," in loc:
        return loc
    if not isinstance(address, dict):
        return loc
    postal = address.get("postalAddress") or {}
    country = (postal.get("addressCountry") or "").strip()
    if not country:
        return loc
    return f"{loc}, {country}"


def ashby_to_rows(
    jobs: list[dict],
    *,
    site_label: str,
    company: str,
    company_url: str = "",
) -> list[dict]:
    """Map Ashby jobs[] payload → jobs.db row shape.

    Ashby job schema (posting-api):
      id, title, location, address.postalAddress.*, secondaryLocations[],
      jobUrl, applyUrl, publishedAt, isListed, isRemote, workplaceType,
      employmentType, department, team, descriptionHtml, descriptionPlain,
      compensation (when ?includeCompensation=true)

    We drop `isListed=false` rows — Ashby's equivalent of SimplifyJobs's
    `is_visible=false`. `applyUrl` is preferred over `jobUrl` since it
    sends the user straight to the application form. Bare-city
    `location` strings (no comma) are enriched with the address's country
    so the location classifier can rank them — see `_ashby_enrich_location`.
    """
    rows: list[dict] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        if j.get("isListed") is False:
            continue
        url = (j.get("applyUrl") or j.get("jobUrl") or "").strip()
        if not url:
            continue
        loc_strings: list[str] = []
        primary = _ashby_enrich_location(
            (j.get("location") or "").strip(), j.get("address")
        )
        if primary:
            loc_strings.append(primary)
        for sec in (j.get("secondaryLocations") or []):
            if not isinstance(sec, dict):
                continue
            s = _ashby_enrich_location(
                (sec.get("location") or "").strip(), sec.get("address")
            )
            if s and s not in loc_strings:
                loc_strings.append(s)

        is_remote: Any = j.get("isRemote")
        if is_remote is not True and is_remote is not False:
            is_remote = None

        description = j.get("descriptionPlain") or None
        rows.append(
            make_row(
                site_label=site_label,
                job_url=url,
                company=company,
                title=j.get("title") or "",
                location_strings=loc_strings,
                company_url=company_url,
                posted_iso=j.get("publishedAt"),
                is_remote=is_remote,
                description=description,
            )
        )
    return rows
