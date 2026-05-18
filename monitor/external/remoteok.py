"""Pull remote-only jobs from RemoteOK's public JSON API.

RemoteOK exposes a free public JSON feed (no key required) at
`https://remoteok.com/api`. The response is a JSON array; the first
element is a legal-notice object with a `legal` field, and all
subsequent elements are job postings.

Schema (verified by inspecting the live feed)::

    [
      {"legal": "By accessing this API ..."},
      {
        "id": "1234567",
        "url": "https://remoteok.com/remote-jobs/...",
        "company": "Stripe",
        "company_logo": "...",
        "position": "Senior Software Engineer",
        "tags": ["python", "remote", "senior"],
        "logo": "...",
        "description": "<html>...</html>",
        "location": "Worldwide",
        "salary_min": 100000,
        "salary_max": 150000,
        "date": "2024-01-15T12:00:00+00:00",
        "epoch": 1705320000,
        "verified": true
      },
      ...
    ]

Heavier crypto / web3 noise than Remotive — the curated allowlist
filter on the slice render handles the quality gate downstream.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Iterable

from monitor.external import build_headers, http_get


DEFAULT_URL = "https://remoteok.com/api"

log = logging.getLogger(__name__)


def fetch_listings(url: str = DEFAULT_URL, timeout: int = 60) -> list[dict]:
    """Download and parse the RemoteOK feed. Returns [] on any error.

    Skips the leading legal-notice object so callers get a clean list
    of job dicts.
    """
    log.info("remoteok: GET %s", url)
    try:
        result = http_get(
            url,
            source_label="remoteok",
            headers=build_headers(referer="https://remoteok.com/"),
            timeout=timeout,
        )
    except Exception as e:
        log.exception("remoteok: fetch failed: %s", e)
        return []
    try:
        payload = json.loads(result.body or b"")
    except Exception as e:
        log.exception("remoteok: JSON parse failed: %s", e)
        return []
    if not isinstance(payload, list):
        log.warning(
            "remoteok: expected list, got %s",
            type(payload).__name__,
        )
        return []
    # First element is a legal notice — drop anything with `legal` key
    # and no `id` / `url`, since the schema is documented but not
    # contractually enforced.
    jobs = [
        r for r in payload
        if isinstance(r, dict) and (r.get("id") or r.get("url"))
        and not r.get("legal")
    ]
    log.info("remoteok: %d listings parsed", len(jobs))
    return jobs


# Same suffix-biased region classifier as remotive, applied to the
# `location` field. Single shared lexicon would be tidier; duplicated
# for now because both schemas have their own location-field semantics
# and the slight token differences (RemoteOK uses "Remote (US)"
# heavily) want per-source tuning.
_NA_TOKENS = (
    "usa", "u.s.", "united states", "us only", "us-only", "us-based",
    "us based", "americas", "north america", "canada", "remote (us)",
    "remote-us", "remote, us", "us / canada",
)
_EMEA_TOKENS = (
    "emea", "europe", "european union", "eu only", "eu-only",
    "eu-based", "uk", "united kingdom", "germany", "france", "ireland",
    "netherlands", "spain", "italy", "sweden", "poland", "portugal",
    "switzerland", "austria", "denmark", "finland", "norway",
    "remote (europe)", "remote-eu", "remote, eu",
)


def _classify_remote_location(raw: str) -> str:
    """Same shape as remotive's classifier — see that module's docstring."""
    text = (raw or "").lower()
    if not text:
        return "other"
    emea_hit = any(tok in text for tok in _EMEA_TOKENS)
    if any(tok in text for tok in _NA_TOKENS):
        return "north_america"
    if emea_hit:
        return "emea"
    return "other"


def _parse_iso_date(raw) -> str | None:
    """RemoteOK ships both `date` (ISO8601) and `epoch` (unix seconds).
    Prefer `date`, fall back to `epoch`. Returns 'YYYY-MM-DD' or None."""
    if isinstance(raw, str) and raw:
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
        if m:
            return m.group(1)
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return None
    if isinstance(raw, (int, float)) and raw > 0:
        try:
            return datetime.fromtimestamp(int(raw), tz=timezone.utc).date().isoformat()
        except (OSError, ValueError):
            return None
    return None


def to_rows(
    listings: Iterable[dict],
    *,
    site_label: str = "remoteok",
    allowed_regions: set[str] | None = None,
) -> list[dict]:
    """Map RemoteOK schema → our jobs.db row shape."""
    rows: list[dict] = []
    skipped_region = 0
    for r in listings:
        if not isinstance(r, dict):
            continue
        url = (r.get("url") or "").strip()
        if not url:
            continue

        loc_raw = (r.get("location") or "").strip()
        region = _classify_remote_location(loc_raw)
        if allowed_regions is not None and region not in allowed_regions:
            skipped_region += 1
            continue

        location = loc_raw or "Worldwide"
        if "remote" not in location.lower():
            location = f"Remote · {location}"

        sal_min = r.get("salary_min")
        sal_max = r.get("salary_max")
        try:
            sal_min = int(sal_min) if sal_min not in (None, "", 0) else None
        except (TypeError, ValueError):
            sal_min = None
        try:
            sal_max = int(sal_max) if sal_max not in (None, "", 0) else None
        except (TypeError, ValueError):
            sal_max = None

        # Use date first, fall back to epoch.
        date_posted = _parse_iso_date(r.get("date")) or _parse_iso_date(r.get("epoch"))

        tags = r.get("tags") or []
        category = ", ".join(str(t) for t in tags[:3]) if isinstance(tags, list) else None

        rows.append(
            {
                "job_url": url,
                "site": site_label,
                "title": r.get("position") or r.get("title") or "",
                "company": r.get("company") or "",
                "company_url": "",
                "location": location,
                "is_remote": True,
                "date_posted": date_posted,
                "description": r.get("description") or None,
                "min_amount": sal_min,
                "max_amount": sal_max,
                "currency": "USD" if sal_min or sal_max else None,
                "salary_interval": "yearly" if sal_min or sal_max else None,
                "region": region,
                "source_category": category,
            }
        )

    if skipped_region:
        log.info("remoteok: dropped %d rows outside allowed_regions", skipped_region)
    return rows
