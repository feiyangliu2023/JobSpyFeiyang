"""Pull jobs from speedyapply/2026-SWE-College-Jobs.

Parses the 4 hand-curated MD files in the repo (README.md = USA interns,
NEW_GRAD_USA.md, INTERN_INTL.md, NEW_GRAD_INTL.md). Each file contains
markdown tables wrapped in `<!-- TABLE_*_START -->` / `<!-- TABLE_*_END -->`
markers — the same convention JOBS.md uses, so the parsing surface is
small and stable.

Schema differs slightly per file:
  - new-grad: `| Company | Position | Location | Salary | Posting | Age |`
  - intern:   `| Company | Position | Location | Posting | Age |`

The Company cell carries the company URL as a wrapping `<a>`. The Posting
cell carries the apply URL as a wrapping `<a>` around an `<img alt="Apply">`.

speedyapply doesn't scrape — it's a curated render of SimplifyJobs + extras.
Net value over the existing simplify_newgrad / simplify_intern feeds is:
  - FAANG / Quant / Other categorisation in the source (not used here yet,
    but stored on `source_category` for later).
  - Salary column on new-grad postings (SimplifyJobs's listings.json has
    no salary field — speedyapply enriches it).
  - The handful of postings that haven't yet been picked up by the canonical
    SimplifyJobs feed (mirrors the vanshb03 fork's value-add).

Signature-based dedup in `render_md` collapses any role that also appears
in SimplifyJobs's feed; SimplifyJobs wins because it ranks above speedyapply
in `_SOURCE_PRIORITY` (raw upstream vs renderer of upstream).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable

from monitor.external import build_headers, http_get
from monitor.external.locations import classify_locations


log = logging.getLogger(__name__)


SPEEDYAPPLY_REPO = "speedyapply/2026-SWE-College-Jobs"
_BASE_URL = f"https://raw.githubusercontent.com/{SPEEDYAPPLY_REPO}/main"

# (url, kind, default_region) per file. `kind` is propagated as part of
# the site_label so `_classify_intern_or_newgrad` in render_md.py routes
# rows correctly without a title regex fallback.
SPEEDYAPPLY_PRESETS: dict[str, tuple[str, str, str]] = {
    "intern_usa":   (f"{_BASE_URL}/README.md",         "intern",  "north_america"),
    "newgrad_usa":  (f"{_BASE_URL}/NEW_GRAD_USA.md",   "newgrad", "north_america"),
    "intern_intl":  (f"{_BASE_URL}/INTERN_INTL.md",    "intern",  "other"),
    "newgrad_intl": (f"{_BASE_URL}/NEW_GRAD_INTL.md",  "newgrad", "other"),
}


# Each MD file has 1-3 `<!-- TABLE_*_START -->` blocks (FAANG / Quant / Other).
# Header names vary (`TABLE_FAANG_START`, `TABLE_QUANT_START`, bare `TABLE_START`),
# so the regex tolerates an optional `_<TAG>` segment.
_TABLE_BLOCK_RE = re.compile(
    r"<!--\s*TABLE(?:_[A-Z0-9]+)?_START\s*-->(.*?)<!--\s*TABLE(?:_[A-Z0-9]+)?_END\s*-->",
    re.DOTALL | re.IGNORECASE,
)
_COMPANY_RE = re.compile(
    r'<a\s+href="([^"]+)"[^>]*>\s*<strong>([^<]+)</strong>\s*</a>',
    re.IGNORECASE,
)
_APPLY_RE = re.compile(
    r'<a\s+href="([^"]+)"[^>]*>\s*<img[^>]*alt="Apply"[^>]*/?>\s*</a>',
    re.IGNORECASE,
)
_AGE_RE = re.compile(r"(\d+)\s*d", re.IGNORECASE)

# Salary cell example: "$168k/yr". `k` = thousands, `m` = millions. Year/hour/
# month intervals map straight onto jobs.db's `salary_interval`. The MD only
# ever ships a single point estimate (not a range), so `min_amount` and
# `max_amount` get the same value.
_SALARY_RE = re.compile(
    r"\$\s*([\d.,]+)\s*([kKmM])?(?:/(?P<interval>yr|hr|mo))?",
)
_SALARY_INTERVAL_MAP = {"yr": "yearly", "hr": "hourly", "mo": "monthly"}


def fetch_md(url: str, timeout: int = 60) -> str:
    """Download one MD file. Returns "" on any error so caller bails cleanly.

    Mirrors `simplify.fetch_listings`'s error-swallowing posture: one bad
    fetch shouldn't kill the whole monitor run.
    """
    log.info("speedyapply: GET %s", url)
    try:
        result = http_get(
            url,
            source_label="speedyapply",
            headers=build_headers(),
            timeout=timeout,
        )
    except Exception as e:
        log.exception("speedyapply: fetch failed: %s", e)
        return ""
    return (result.body or b"").decode("utf-8", errors="replace")


def parse_md_listings(text: str) -> list[dict]:
    """Extract per-row dicts from the MD tables in `text`.

    Returns a list of {company, company_url, title, location, salary,
    job_url, age_days}. `salary` is the raw cell string (e.g. "$168k/yr")
    or None when the table has no Salary column; `age_days` is the int
    parsed out of the "Age" column ("11d" → 11).

    Continuation rows (some forks use `↳` to indicate the same company
    spans multiple locations) drop here because the Company cell has no
    `<a><strong>` markup. Malformed rows where the apply link is missing
    also drop — there's no usable PK without a job_url.
    """
    rows: list[dict] = []
    if not text:
        return rows

    for block_match in _TABLE_BLOCK_RE.finditer(text):
        block = block_match.group(1)
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        # Locate the header row (the one starting with `| Company`). The
        # separator row (`|---|---|...`) sits immediately after; rows below
        # that are data.
        header_idx = next(
            (i for i, ln in enumerate(lines)
             if ln.startswith("|") and "Company" in ln and "Position" in ln),
            None,
        )
        if header_idx is None:
            continue

        headers = [c.strip() for c in lines[header_idx].strip("|").split("|")]
        has_salary = any(h == "Salary" for h in headers)

        # Skip header + the `|---|---|` separator line, then iterate data rows.
        for ln in lines[header_idx + 2:]:
            if not ln.startswith("|"):
                continue
            cells = [c.strip() for c in ln.strip("|").split("|")]
            # Required columns: company, position, location, [salary], apply, age.
            min_cols = 6 if has_salary else 5
            if len(cells) < min_cols:
                continue

            apply_idx = 4 if has_salary else 3
            age_idx = 5 if has_salary else 4

            company_m = _COMPANY_RE.search(cells[0])
            if not company_m:
                # No <strong>Company</strong> markup — continuation row or junk
                continue
            apply_m = _APPLY_RE.search(cells[apply_idx])
            if not apply_m:
                continue
            age_m = _AGE_RE.search(cells[age_idx])
            age_days = int(age_m.group(1)) if age_m else None

            rows.append({
                "company": company_m.group(2).strip(),
                "company_url": company_m.group(1).strip(),
                "title": cells[1],
                "location": cells[2],
                "salary": cells[3] if has_salary else None,
                "job_url": apply_m.group(1).strip(),
                "age_days": age_days,
            })

    log.info("speedyapply: parsed %d rows", len(rows))
    return rows


def _parse_salary(text: str | None) -> tuple[int | None, int | None, str | None, str | None]:
    """Parse a speedyapply salary cell.

    Returns (min_amount, max_amount, currency, salary_interval). The MD
    ships a single number (not a range) so min == max. The "$" prefix is
    treated as USD; other currency symbols (£/€) appear too rarely on
    speedyapply to bother distinguishing here — we leave currency NULL
    when no `$` is present so downstream code doesn't get the wrong tag.
    """
    if not text:
        return None, None, None, None
    m = _SALARY_RE.search(text)
    if not m:
        return None, None, None, None
    try:
        amount = float(m.group(1).replace(",", ""))
    except ValueError:
        return None, None, None, None
    suffix = (m.group(2) or "").lower()
    if suffix == "k":
        amount *= 1_000
    elif suffix == "m":
        amount *= 1_000_000
    interval = _SALARY_INTERVAL_MAP.get((m.group("interval") or "").lower())
    currency = "USD" if "$" in text else None
    val = int(amount)
    return val, val, currency, interval


def fetch_listings(file_key: str, timeout: int = 60) -> list[dict]:
    """Fetch + parse one preset (`intern_usa` / `newgrad_usa` / …).

    Lookup-error returns []; caller will record the source as DEGRADED
    but won't crash the run.
    """
    preset = SPEEDYAPPLY_PRESETS.get(file_key)
    if not preset:
        log.warning("speedyapply: unknown file key %r", file_key)
        return []
    url, _, _ = preset
    text = fetch_md(url, timeout=timeout)
    return parse_md_listings(text)


def to_rows(
    listings: Iterable[dict],
    site_label: str,
    default_region: str = "north_america",
    allowed_regions: set[str] | None = None,
) -> list[dict]:
    """Map speedyapply parsed rows → our jobs.db row shape.

    `site_label` is what lands in the `site` column. Use one of the four
    SPEEDYAPPLY_PRESETS keys (prefixed with `speedyapply_` in config) so
    `_classify_intern_or_newgrad` picks the right bucket from substring
    matching — "intern" or "newgrad" in the label is the signal.

    Region is auto-classified from the location string. INTL files mostly
    yield "other" → those get dropped when `allowed_regions={emea, north_america}`.
    USA files use `default_region="north_america"` so locations the
    classifier doesn't recognise (e.g. "Milwaukee Wisconsin United States
    of America" — no comma to split on) still bucket correctly.
    """
    out: list[dict] = []
    now = datetime.now(timezone.utc)
    skipped_region = 0

    for r in listings:
        url = (r.get("job_url") or "").strip()
        if not url:
            continue

        loc_str = (r.get("location") or "").strip()
        if loc_str:
            _, region = classify_locations([loc_str])
            if region == "other":
                region = default_region
        else:
            region = default_region

        if allowed_regions is not None and region not in allowed_regions:
            skipped_region += 1
            continue

        age_days = r.get("age_days")
        date_posted = None
        if isinstance(age_days, int) and age_days >= 0:
            date_posted = (now - timedelta(days=age_days)).date().isoformat()

        min_amount, max_amount, currency, salary_interval = _parse_salary(
            r.get("salary")
        )

        out.append({
            "job_url": url,
            "site": site_label,
            "title": (r.get("title") or "").strip(),
            "company": (r.get("company") or "").strip(),
            "company_url": (r.get("company_url") or "").strip(),
            "location": loc_str,
            # speedyapply doesn't expose a remote flag; let COALESCE in upsert
            # preserve any prior value from another source.
            "is_remote": None,
            "date_posted": date_posted,
            # MD ships no description — same situation as SimplifyJobs's
            # listings.json. `min_description_chars` must be in skip_filters.
            "description": None,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "currency": currency,
            "salary_interval": salary_interval,
            "region": region,
            "source_category": None,
        })

    if skipped_region:
        log.info(
            "speedyapply (%s): dropped %d listings outside allowed_regions",
            site_label, skipped_region,
        )
    return out
