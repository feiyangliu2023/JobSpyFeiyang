"""Pull remote-only jobs from Remotive's public API.

Remotive is a curated remote-jobs board with a free public JSON API
(no key required). Every posting is remote-eligible by definition —
no further filtering needed. The schema is stable and well-documented:

    https://remotive.com/api/remote-jobs
    https://remotive.com/api-documentation

Response shape::

    {
      "0-legal-notice": "...",
      "job-count": 1234,
      "jobs": [
        {
          "id": 1234567,
          "url": "https://remotive.com/remote-jobs/...",
          "title": "Senior Software Engineer",
          "company_name": "Stripe",
          "company_logo": "...",
          "category": "Software Development",
          "tags": ["python", "remote"],
          "job_type": "full_time",
          "publication_date": "2024-01-15T12:00:00",
          "candidate_required_location": "USA Only",
          "salary": "100000",
          "description": "<html>...</html>"
        }
      ]
    }

The whole table fits in a single ~500KB GET — pagination is not
exposed by the public API. We fetch and parse once per run.

`region` is auto-classified from `candidate_required_location` so a
"USA Only" Stripe role lands in NA and an "EU only" Mistral role
lands in EMEA. "Worldwide" / "Anywhere" / unknown phrasings classify
as `other`; the remote-jobs slice doesn't filter on region so they
still surface.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Iterable

from monitor.external import build_headers, http_get


DEFAULT_URL = "https://remotive.com/api/remote-jobs"

log = logging.getLogger(__name__)


def fetch_listings(url: str = DEFAULT_URL, timeout: int = 60) -> list[dict]:
    """Download and parse Remotive's job feed. Returns [] on any error.

    Same retry shape as simplify.py — bounded retries on transient
    failure, hard failures return []. The whole monitor run keeps
    going if Remotive is down.
    """
    log.info("remotive: GET %s", url)
    try:
        result = http_get(
            url,
            source_label="remotive",
            headers=build_headers(referer="https://remotive.com/"),
            timeout=timeout,
        )
    except Exception as e:
        log.exception("remotive: fetch failed: %s", e)
        return []
    try:
        payload = json.loads(result.body or b"")
    except Exception as e:
        log.exception("remotive: JSON parse failed: %s", e)
        return []
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        log.warning(
            "remotive: expected dict with `jobs` list, got %s",
            type(payload).__name__,
        )
        return []
    log.info("remotive: %d listings parsed", len(jobs))
    return jobs


# Region classification on Remotive's `candidate_required_location` strings.
# Suffix-biased so "Latin America / USA" classifies as north_america (the
# last region wins), matching the suffix-biased style of
# monitor.external.locations. Comparisons are case-folded substring on
# word-ish boundaries.
_NA_TOKENS = (
    "usa", "u.s.", "united states", "us only", "us-only", "us-based",
    "us based", "americas", "north america", "canada", "ca only",
    "ca-only", "ca-based", "anywhere in north america",
)
_EMEA_TOKENS = (
    "emea", "europe", "european union", "eu only", "eu-only",
    "eu-based", "uk", "united kingdom", "germany", "france", "ireland",
    "netherlands", "spain", "italy", "sweden", "poland", "portugal",
    "switzerland", "austria", "denmark", "finland", "norway",
    "anywhere in europe",
)


def _classify_remote_location(raw: str) -> str:
    """Map a Remotive `candidate_required_location` string to our region.

    Substring match on case-folded text. Returns one of
    `'north_america'`, `'emea'`, or `'other'`. Worldwide / Anywhere /
    blank values land in `'other'`, which the existing
    `allowed_regions=[emea, north_america]` config will then drop —
    so to surface worldwide roles a source must set
    `allowed_regions: [emea, north_america, other]` (or omit it).
    """
    text = (raw or "").lower()
    if not text:
        return "other"
    # NA wins ties to "anywhere in the americas / EU"-style strings:
    # check it second so EMEA only wins when the string is unambiguously
    # European.
    if any(tok in text for tok in _EMEA_TOKENS):
        emea_hit = True
    else:
        emea_hit = False
    if any(tok in text for tok in _NA_TOKENS):
        return "north_america"
    if emea_hit:
        return "emea"
    return "other"


_PUB_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


def _parse_publication_date(raw: str | None) -> str | None:
    """Remotive publication_date is ISO8601 like '2024-01-15T12:00:00'.

    We only need the date portion — store as 'YYYY-MM-DD' to match
    SimplifyJobs. Returns None if the input doesn't look like a date.
    """
    if not raw:
        return None
    m = _PUB_DATE_RE.match(str(raw))
    if m:
        return m.group(1)
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date().isoformat()
    except (TypeError, ValueError):
        return None


def _parse_salary(raw: str | None) -> tuple[int | None, int | None]:
    """Remotive `salary` is a free-text string like '100000' / '$80k - $120k'
    / 'USD 60,000 - 90,000'. We extract min/max integers from it best-effort
    and return USD assumed (Remotive doesn't expose currency separately).

    Returns (min, max) — either may be None. Bare single numbers populate
    only `min`. Numbers below 1000 are interpreted as "k" (e.g. "80" → 80000),
    matching how RemoteOK / Remotive abbreviate.
    """
    if not raw or not isinstance(raw, str):
        return None, None
    # Strip common currency / period markers and pull all digit groups.
    nums: list[int] = []
    for m in re.finditer(r"(\d[\d,\.]*)\s*([kKmM])?", raw):
        try:
            base = float(m.group(1).replace(",", "").replace(".", "."))
        except ValueError:
            continue
        suffix = (m.group(2) or "").lower()
        if suffix == "k":
            base *= 1000
        elif suffix == "m":
            base *= 1_000_000
        if base < 1000:
            # Bare "80" alongside k-suffix peers means 80k. Treat as k.
            base *= 1000
        if base > 1_000_000:
            # Anything beyond $1M/yr is almost certainly garbage data.
            continue
        nums.append(int(base))
        if len(nums) >= 2:
            break
    if not nums:
        return None, None
    if len(nums) == 1:
        return nums[0], None
    return min(nums), max(nums)


def to_rows(
    listings: Iterable[dict],
    *,
    site_label: str = "remotive",
    allowed_regions: set[str] | None = None,
) -> list[dict]:
    """Map Remotive schema → our jobs.db row shape.

    Drops listings missing a usable URL (our DB PK). `is_remote=True`
    is hardcoded on every row — Remotive is a remote-only board, so
    every posting matches the remote-jobs slice's downstream filter
    even when the location string is something weird like "Anywhere".
    """
    rows: list[dict] = []
    skipped_region = 0
    for r in listings:
        if not isinstance(r, dict):
            continue
        url = (r.get("url") or "").strip()
        if not url:
            continue

        loc_raw = (r.get("candidate_required_location") or "").strip()
        region = _classify_remote_location(loc_raw)
        if allowed_regions is not None and region not in allowed_regions:
            skipped_region += 1
            continue

        # Visible location string: prefer the source's phrasing; prepend
        # "Remote · " so the slice's title/location remote-marker regex
        # matches even when the source string is something like "USA Only".
        location = loc_raw or "Worldwide"
        if "remote" not in location.lower():
            location = f"Remote · {location}"

        sal_min, sal_max = _parse_salary(r.get("salary"))

        rows.append(
            {
                "job_url": url,
                "site": site_label,
                "title": r.get("title") or "",
                "company": r.get("company_name") or "",
                "company_url": "",
                "location": location,
                "is_remote": True,
                "date_posted": _parse_publication_date(r.get("publication_date")),
                "description": r.get("description") or None,
                "min_amount": sal_min,
                "max_amount": sal_max,
                "currency": "USD" if sal_min or sal_max else None,
                "salary_interval": "yearly" if sal_min or sal_max else None,
                "region": region,
                "source_category": r.get("category") or None,
            }
        )

    if skipped_region:
        log.info("remotive: dropped %d rows outside allowed_regions", skipped_region)
    return rows
