"""Pull jobs from SimplifyJobs-format listings.json files.

This module handles BOTH SimplifyJobs/New-Grad-Positions and the related
Summer2026-Internships repo (and any future fork using the same schema).
The JSON is hand-curated, updated multiple times per day, and is the
upstream feeding speedyapply / coderquad-simplify / similar trackers.

Each listing's `region` is auto-classified from its `locations[]` array
via monitor.external.locations — so a "London, UK" Graphcore role winds
up in our EMEA section even though the source repo is mostly US-centric.
Sources can pass `allowed_regions` to drop, e.g., APAC roles entirely.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable

from monitor.external import build_headers, http_get
from monitor.external.locations import classify_locations

DEFAULT_URL = (
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions"
    "/dev/.github/scripts/listings.json"
)

# Known SimplifyJobs-format upstreams. Used as `type: <key>` in the
# config.yaml `external_sources` block so we can dispatch to the right
# fetch / map logic without hardcoding URLs.
SIMPLIFY_PRESETS = {
    "simplify_newgrad": (
        "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions"
        "/dev/.github/scripts/listings.json"
    ),
    "simplify_intern": (
        "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships"
        "/dev/.github/scripts/listings.json"
    ),
}

log = logging.getLogger(__name__)


def fetch_listings(url: str = DEFAULT_URL, timeout: int = 60) -> list[dict]:
    """Download and parse the listings.json. Returns [] on any error.

    Uses the shared http_get helper, so a transient connection blip or a
    429 / 5xx gets up to 3 bounded retries (2s / 5s / 10s) before we
    give up. Persistent failures still return [] — one bad fetch
    shouldn't kill the whole monitor run, same as JobSpy's run_search.
    """
    log.info("simplify: GET %s", url)
    try:
        result = http_get(
            url,
            source_label="simplify",
            headers=build_headers(),
            timeout=timeout,
        )
    except Exception as e:
        log.exception("simplify: fetch failed: %s", e)
        return []
    try:
        listings = json.loads(result.body or b"")
    except Exception as e:
        log.exception("simplify: JSON parse failed: %s", e)
        return []
    if not isinstance(listings, list):
        log.warning("simplify: expected list, got %s", type(listings).__name__)
        return []
    log.info("simplify: %d listings parsed", len(listings))
    return listings


def _epoch_to_iso_date(epoch) -> str | None:
    """Unix epoch seconds → 'YYYY-MM-DD' (date only, UTC). None on failure."""
    if epoch in (None, 0):
        return None
    try:
        return (
            datetime.fromtimestamp(int(epoch), tz=timezone.utc).date().isoformat()
        )
    except (TypeError, ValueError, OSError):
        return None


def to_rows(
    listings: Iterable[dict],
    site_label: str = "simplify",
    default_region: str = "north_america",
    allowed_regions: set[str] | None = None,
) -> list[dict]:
    """Map SimplifyJobs schema → our jobs.db row shape.

    `site_label` lands in the `site` column so downstream code (priority
    ranking, debug filtering) can tell which repo a row came from —
    'simplify' for new-grad, 'simplify_intern' for the Summer 2026 repo.

    Region is auto-classified from `locations[]`. When the listing has no
    location at all we fall back to `default_region`. If `allowed_regions`
    is provided, rows whose classified region is outside it are dropped
    silently — useful for filtering out APAC postings without polluting
    the user's main views.

    Drops listings that SimplifyJobs has marked inactive/hidden or that
    lack a usable `url` (our DB PK).

    SimplifyJobs schema reference (verified by inspecting raw listings.json):
        id, source, category, company_name, title, active, date_updated,
        date_posted (unix epoch), url, locations[], company_url, is_visible,
        sponsorship, degrees[]
    """
    rows: list[dict] = []
    skipped_region = 0
    for r in listings:
        if not isinstance(r, dict):
            continue
        if not r.get("active", True):
            continue
        if not r.get("is_visible", True):
            continue
        url = (r.get("url") or "").strip()
        if not url:
            continue

        locations_raw = r.get("locations") or []
        if not isinstance(locations_raw, list):
            locations_raw = [str(locations_raw)] if locations_raw else []

        if locations_raw:
            _, region = classify_locations(locations_raw)
        else:
            region = default_region

        if allowed_regions is not None and region not in allowed_regions:
            skipped_region += 1
            continue

        # Cap at 3 visible locations — some listings have a dozen which
        # would blow up table column width.
        location = " · ".join(str(x) for x in locations_raw[:3]).strip()

        rows.append(
            {
                "job_url": url,
                "site": site_label,
                "title": r.get("title") or "",
                "company": r.get("company_name") or "",
                "company_url": r.get("company_url") or "",
                "location": location,
                # No remote flag in this schema; leave None so COALESCE in
                # upsert preserves any prior value.
                "is_remote": None,
                "date_posted": _epoch_to_iso_date(r.get("date_posted")),
                "description": None,  # listings.json carries no description
                "min_amount": None,
                "max_amount": None,
                "currency": None,
                "salary_interval": None,
                "region": region,
                # SimplifyJobs's category field — Software / AI/ML/Data /
                # Quant / Hardware / PM. Stored for future filtering;
                # renderer doesn't use it yet.
                "source_category": (r.get("category") or "").strip() or None,
            }
        )
    if skipped_region:
        log.info(
            "simplify (%s): dropped %d listings outside allowed_regions",
            site_label, skipped_region,
        )
    return rows
