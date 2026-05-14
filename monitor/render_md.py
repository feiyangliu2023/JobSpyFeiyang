"""Render JOBS.md from the active rows in jobs.db.

Output layout: one comprehensive table per region (EMEA / North America),
sorted newest-first. No tier sub-buckets, no freshness sub-sections —
the Age column plus a date-descending sort is enough.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable


# External "Apply" button image, mirroring the SimplifyJobs new-grad repo.
# If imgur ever rots we just see broken images — links still work.
_APPLY_IMG = (
    '<img src="https://i.imgur.com/JpkfjIq.png" alt="Apply" width="70"/>'
)


# Liveness-aware visibility. Four behaviors:
#   - 'ok'                — render normally (verified live).
#   - 'redirect' / NULL   — render with a "(?)" suffix on the apply cell
#                           ("not yet verified or possibly stale"). NULL
#                           covers rows we've never checked.
#   - 'timeout' / 'error' — drop this render cycle entirely. They retry
#                           next sweep; a transient network blip here
#                           would otherwise hide an actually-live row
#                           behind a "(?)" mark forever.
#   - '404'               — drop, except within 24h of being marked
#                           (transient-error grace window — matches the
#                           rule from the original liveness wiring).
_LIVENESS_HIDE_GRACE_HOURS = 24
_LIVENESS_DROP_THIS_RENDER = ("timeout", "error")


def _liveness_visible(r: dict, now: datetime | None = None) -> bool:
    """True iff this row should appear in the rendered tables.

    Drop the row when:
      - liveness_status is 'timeout' / 'error' (this render cycle only)
      - liveness_status is '404' AND liveness_checked_at is older than
        24h (the grace window keeps freshly-dead rows visible in case
        the 404 turns out to be transient)
    Otherwise — 'ok', 'redirect', NULL, or fresh 404 — keep visible.
    The apply-cell "(?)" decoration for unverified rows lives in
    `_render_section`, not here.
    """
    status = (r.get("liveness_status") or "").lower()
    if status in _LIVENESS_DROP_THIS_RENDER:
        return False
    if status != "404":
        return True
    checked_at = r.get("liveness_checked_at")
    if not checked_at:
        return True
    try:
        dt = datetime.fromisoformat(str(checked_at).replace("Z", "+00:00"))
    except ValueError:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if now is None:
        now = datetime.now(timezone.utc)
    return dt >= now - timedelta(hours=_LIVENESS_HIDE_GRACE_HOURS)


def _filter_liveness(rows: Iterable[dict]) -> list[dict]:
    """Apply the liveness visibility filter to a row iterable."""
    now = datetime.now(timezone.utc)
    return [r for r in rows if _liveness_visible(r, now)]


# Suffix appended to the apply-button cell for rows that aren't verified
# 'ok'. Renders as a small superscript "(?)" with a tooltip. We deliberately
# don't use a status-specific marker (different for redirect vs NULL) —
# from the user's perspective they're both "not confirmed live".
_LIVENESS_UNVERIFIED_MARKER = (
    ' <sup title="not yet verified live — may be stale">(?)</sup>'
)


def _is_live(r: dict) -> bool:
    return (r.get("liveness_status") or "").lower() == "ok"


def days_since(iso: str | None) -> int:
    if not iso:
        return 0
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    return max(0, delta.days)


_CURRENCY_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "£", "CHF": "CHF ", "SEK": "kr ",
    "NOK": "kr ", "DKK": "kr ", "PLN": "zł ", "JPY": "¥",
}

_INTERVAL_SHORT = {
    "yearly": "yr", "annual": "yr", "annually": "yr",
    "monthly": "mo", "weekly": "wk", "daily": "d", "hourly": "hr",
}


def fmt_salary(min_amt, max_amt, currency, interval) -> str:
    """`£60k-80k/yr`, `$67/hr`, or `""` when nothing useful is available."""
    if min_amt in (None, 0) and max_amt in (None, 0):
        return ""
    sym = _CURRENCY_SYMBOLS.get((currency or "").upper(), (currency or "").upper())
    if sym and not sym.endswith(" ") and len(sym) > 1:
        sym += " "
    period = _INTERVAL_SHORT.get((interval or "").lower(), "")

    def short(n):
        if n in (None, 0):
            return ""
        try:
            n = float(n)
        except (TypeError, ValueError):
            return ""
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}m".replace(".0m", "m")
        if n >= 1000:
            return f"{int(round(n / 1000))}k"
        return str(int(round(n)))

    s_min, s_max = short(min_amt), short(max_amt)
    if s_min and s_max and s_min != s_max:
        body = f"{s_min}-{s_max}"
    else:
        body = s_min or s_max
    return f"{sym}{body}/{period}" if period else f"{sym}{body}"


def _md_escape_cell(s: str) -> str:
    """Make a string safe to drop into a markdown table cell."""
    return (s or "").replace("|", "\\|").replace("\n", " ").strip()


def _has_salary(r: dict) -> bool:
    """True iff this row carries a non-zero min or max salary."""
    for k in ("min_amount", "max_amount"):
        v = r.get(k)
        if v not in (None, 0):
            try:
                if float(v) > 0:
                    return True
            except (TypeError, ValueError):
                pass
    return False


def _row_age_days(r: dict) -> int:
    """Prefer real `date_posted` from the source, fall back to `first_seen`
    (when JobSpy didn't return a posting date — common for LinkedIn).

    `first_seen` overstates "freshness" for jobs the scraper has never seen
    before but were posted weeks ago; `date_posted` is the true age."""
    return days_since(r.get("date_posted") or r.get("first_seen"))


def _render_section(rows: list[dict], has_salary: bool) -> str:
    if not rows:
        return "_No active roles in this category._"

    headers = ["Company", "Position", "Location"]
    if has_salary:
        headers.append("Salary")
    headers += ["Posting", "Age"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]

    n_live = 0
    n_unverified = 0
    for r in rows:
        company = _md_escape_cell(r.get("company") or "—")
        title = _md_escape_cell(r.get("title") or "")
        location = _md_escape_cell(r.get("location") or "")
        url = (r.get("job_url") or "#").strip()
        age = _row_age_days(r)
        company_cell = f"**{company}**"
        cu = (r.get("company_url") or "").strip()
        if cu:
            company_cell = f'<a href="{cu}"><strong>{company}</strong></a>'

        if _is_live(r):
            n_live += 1
            marker = ""
        else:
            n_unverified += 1
            marker = _LIVENESS_UNVERIFIED_MARKER
        apply_cell = f'<a href="{url}">{_APPLY_IMG}</a>{marker}'

        cells = [company_cell, title, location]
        if has_salary:
            cells.append(
                fmt_salary(
                    r.get("min_amount"),
                    r.get("max_amount"),
                    r.get("currency"),
                    r.get("salary_interval"),
                )
            )
        cells += [apply_cell, f"{age}d"]
        lines.append("| " + " | ".join(cells) + " |")

    # Per-table footer — helps the reader calibrate how much of the table
    # has been validated. Live = a 2xx HEAD/GET within the last sweep
    # cycle; unverified = NULL (never checked), 'redirect' (heuristic
    # dead-page suspicion — kept visible so the user can judge), or fresh
    # 404 inside the 24h grace window.
    lines.append("")
    lines.append(f"_{n_live} live, {n_unverified} unverified_")
    return "\n".join(lines)


# Region rendering order. EMEA is the user's primary feed (their own
# JobSpy scrape) and stays on top regardless of row count; North America
# is a secondary feed pulled from SimplifyJobs.
_REGION_ORDER: list[tuple[str, str, str]] = [
    # (region_key, anchor_prefix, heading)
    ("emea", "emea", "EMEA"),
    ("north_america", "na", "North America"),
]

_REGION_BLURB = {
    "emea": (
        "JobSpy-driven scrape of EMEA tech hubs (London, Dublin, Amsterdam, "
        "Berlin, Munich, Zurich, Paris, Stockholm)."
    ),
    "north_america": (
        "Pulled daily from "
        "[SimplifyJobs/New-Grad-Positions](https://github.com/SimplifyJobs/New-Grad-Positions) "
        "— hand-curated upstream feeding speedyapply and similar trackers."
    ),
}


def _sort_key(r: dict) -> tuple[str, str]:
    """Newest-first ordering, using the same age signal we render."""
    return (
        r.get("date_posted") or r.get("first_seen") or "",
        r.get("company") or "",
    )


# Source priority for cross-source dedup. Lower index = preferred when
# the same posting (same signature) appears in multiple sources.
#
# Ordering rationale, best → worst:
#   1. `direct:*` — scraped straight off the company's ATS feed
#      (Greenhouse / Ashby / Lever / etc). Zero aggregator lag, direct
#      apply URL, machine-readable. See `monitor/external/direct/`.
#   2. SimplifyJobs feeds — hand-curated by the upstream maintainers,
#      usually link to the company's direct apply URL too, but with
#      hours-to-days of human latency. SimplifyJobs-schema forks
#      (vanshb03_summer2026 etc.) sit at the same tier as the canonical
#      feeds — equally direct URLs, just slightly different coverage.
#   3. JobSpy scrapers — referrals through indeed.com / linkedin.com /
#      glassdoor.com (Indeed in particular is often a redirect chain).
#
# `direct:*` is matched as a PREFIX in `_source_rank` since the site
# label encodes the company name (`direct:anthropic`, `direct:openai`).
_DIRECT_PREFIX = "direct:"
_SOURCE_PRIORITY = [
    "simplify_newgrad",
    "simplify_intern",
    "vanshb03_summer2026",
    "simplify",       # legacy label, kept for old DB rows
    "linkedin",
    "indeed",
    "glassdoor",
    "google",
]


def _source_rank(site: str | None) -> int:
    if not site:
        return len(_SOURCE_PRIORITY) + 2
    s = site.lower()
    # `direct:*` always wins — promoted ABOVE everything in _SOURCE_PRIORITY
    # by returning -1. The bare prefix with nothing after it gets ranked
    # at the end (treat as unknown) so a malformed label doesn't sneak past.
    if s.startswith(_DIRECT_PREFIX) and len(s) > len(_DIRECT_PREFIX):
        return -1
    for i, name in enumerate(_SOURCE_PRIORITY):
        if s == name:
            return i
    return len(_SOURCE_PRIORITY) + 1


def _dedupe_by_signature(rows: list[dict]) -> tuple[list[dict], int]:
    """Collapse rows that share a signature, keep the higher-priority source.

    Callers pass per-region row lists — we never collapse across regions,
    since "Apple, Cupertino" and "Apple, London" are obviously different
    roles even with similar titles.

    Returns (deduped_rows, num_collapsed).
    """
    by_sig: dict[str, dict] = {}
    no_sig: list[dict] = []
    collapsed = 0
    for r in rows:
        sig = (r.get("signature") or "").strip()
        if not sig:
            no_sig.append(r)
            continue
        prev = by_sig.get(sig)
        if prev is None:
            by_sig[sig] = r
            continue
        # Already have one with this signature — keep the higher-priority
        # source. Tie-breaker: keep whichever was scraped first
        # (lower first_seen) so the row that's been around longer wins
        # over a freshly-discovered duplicate.
        prev_rank = _source_rank(prev.get("site"))
        curr_rank = _source_rank(r.get("site"))
        if curr_rank < prev_rank or (
            curr_rank == prev_rank
            and (r.get("first_seen") or "") < (prev.get("first_seen") or "")
        ):
            by_sig[sig] = r
        collapsed += 1
    return list(by_sig.values()) + no_sig, collapsed


def _dedupe_and_sort(
    rows: list[dict],
    sort_key: Callable[[dict], object] | None = None,
) -> list[dict]:
    """Dedup by signature then sort newest-first."""
    deduped, _collapsed = _dedupe_by_signature(rows)
    deduped.sort(key=sort_key or _sort_key, reverse=True)
    return deduped


# Title tokens that mark a row as an internship rather than a new-grad role.
# Substring-matched against a lowercased title — leading prefixes are fine
# (so `intern` matches `internship`/`interns`).
_INTERN_TITLE_TOKENS = (
    "intern",        # "Intern", "Internship", "Software Engineer Intern"
    "placement",     # UK industrial placement
    "year in industry",
    "industrial placement",
    "praktikum",     # German
    "stagiair",      # Dutch
    "becario",       # Spanish
    "tirocinante",   # Italian
    "trainee",       # often (but not always) intern-shaped; close enough
)
# `stage` is the French/Italian word for an internship. We can't substring-
# match it because it would also match real titles like "Backstage Platform
# Engineer" or "Multi-stage Pipeline Engineer". Bare `\bstage\b` isn't enough
# either — `\b` matches at hyphen boundaries, so "Multi-stage" still hits.
# Exclude word chars AND hyphens on either side so only the standalone word
# (whitespace / punctuation / start-of-string boundaries) counts.
_STAGE_RE = re.compile(r"(?<![\w-])stage(?![\w-])")


def _classify_intern_or_newgrad(r: dict) -> str:
    """Return 'intern' or 'newgrad'. Used only for the EMEA entry-level view.

    SimplifyJobs-shaped rows carry the answer in `site`. We substring-match
    rather than equality-check so forks (vanshb03_summer2026, etc.) classify
    naturally: any label containing 'intern' or 'summer202X' is intern;
    any label containing 'newgrad' / 'new-grad' / 'new_grad' is new-grad.
    For JobSpy rows the site is just `linkedin`/`indeed`/etc. — none of
    those substrings match, so we fall through to a title-based check.
    Default → 'newgrad' since the EMEA scrape's `job_type=fulltime` filter
    biases toward new-grad anyway.
    """
    site = (r.get("site") or "").lower()
    if "intern" in site or "summer202" in site:
        return "intern"
    if "newgrad" in site or "new-grad" in site or "new_grad" in site:
        return "newgrad"
    title = (r.get("title") or "").lower()
    for tok in _INTERN_TITLE_TOKENS:
        if tok in title:
            return "intern"
    if _STAGE_RE.search(title):
        return "intern"
    return "newgrad"


_EMEA_KIND_ORDER: list[tuple[str, str]] = [
    ("intern", "Internships"),
    ("newgrad", "New Grad"),
]


def render_emea_entry_level(
    rows: Iterable[dict], output_path: str | Path
) -> int:
    """Broader EMEA entry-level view — same row shape as JOBS.md but no
    company allowlist gate.

    One comprehensive table per kind (Internships / New Grad). Sorted
    newest-first; the Age column carries freshness, so no separate 24h /
    7d sections.
    """
    rows = [r for r in rows if (r.get("region") or "").lower() == "emea"]
    rows = _filter_liveness(rows)
    has_salary = any(_has_salary(r) for r in rows)

    by_kind: dict[str, list[dict]] = {k: [] for k, _ in _EMEA_KIND_ORDER}
    for r in rows:
        by_kind[_classify_intern_or_newgrad(r)].append(r)

    kind_rows: dict[str, list[dict]] = {}
    for kind_key, _ in _EMEA_KIND_ORDER:
        kind_rows[kind_key] = _dedupe_and_sort(by_kind[kind_key])

    visible_total = sum(len(v) for v in kind_rows.values())

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append("# EMEA Entry-Level Roles")
    out.append("")
    kind_summaries = [
        f"[{heading}](#{kind_key}) ({len(kind_rows[kind_key])})"
        for kind_key, heading in _EMEA_KIND_ORDER
    ]
    out.append(
        f"Last updated: **{now}** · **{visible_total}** active EMEA "
        f"entry-level roles ({' · '.join(kind_summaries)})."
    )
    out.append("")
    out.append("---")
    out.append("")

    for kind_key, kind_heading in _EMEA_KIND_ORDER:
        marker = f"TABLE_EMEA_{kind_key.upper()}"
        out.append(f'<a name="{kind_key}"></a>')
        out.append(f"## {kind_heading}")
        out.append("")
        out.append(f"<!-- {marker}_START -->")
        out.append(_render_section(kind_rows[kind_key], has_salary))
        out.append(f"<!-- {marker}_END -->")
        out.append("")
        out.append("---")
        out.append("")

    while out and out[-1] in ("", "---"):
        out.pop()
    out.append("")

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
    return visible_total


def render_md(active_rows: Iterable[dict], output_path: str | Path) -> int:
    """Group `active_rows` by region and write the markdown.

    Layout: one comprehensive table per region (EMEA first since that's
    the user's primary scraper, then North America). Sort is newest-first
    so the Age column carries freshness without needing sub-sections.

    Each table is wrapped in `<!-- TABLE_<REGION>_START -->` /
    `<!-- TABLE_<REGION>_END -->` HTML markers so future tooling can do
    partial-replace on a hand-curated outer file.

    The Salary column is hidden globally whenever no active row carries
    salary data (typical for EMEA Indeed scrapes), keeping every table
    consistently shaped.
    """
    rows = _filter_liveness(active_rows)
    has_salary = any(_has_salary(r) for r in rows)

    # Group by region first; anything with an unknown region tag falls
    # into EMEA so legacy DB rows render somewhere visible.
    by_region: dict[str, list[dict]] = {k: [] for k, _, _ in _REGION_ORDER}
    for r in rows:
        region = (r.get("region") or "emea").lower()
        if region not in by_region:
            region = "emea"
        by_region[region].append(r)

    region_rows = {
        region_key: _dedupe_and_sort(by_region[region_key])
        for region_key, _, _ in _REGION_ORDER
    }

    region_visible = {k: len(region_rows[k]) for k, _, _ in _REGION_ORDER}
    visible_total = sum(region_visible.values())

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append("# Junior Tech Roles")
    out.append("")

    region_summaries = [
        f"[{heading}](#{anchor_prefix}) ({region_visible[region_key]})"
        for region_key, anchor_prefix, heading in _REGION_ORDER
    ]
    out.append(
        f"Last updated: **{now}** · **{visible_total}** active roles "
        f"({' · '.join(region_summaries)})."
    )
    out.append("")
    out.append("---")
    out.append("")

    for region_key, anchor_prefix, heading in _REGION_ORDER:
        out.append(f'<a name="{anchor_prefix}"></a>')
        primary_tag = " (primary)" if region_key == "emea" else ""
        out.append(f"## {heading}{primary_tag}")
        out.append("")
        blurb = _REGION_BLURB.get(region_key)
        if blurb:
            out.append(blurb)
            out.append("")

        marker = f"TABLE_{anchor_prefix.upper()}"
        out.append(f"<!-- {marker}_START -->")
        out.append(_render_section(region_rows[region_key], has_salary))
        out.append(f"<!-- {marker}_END -->")
        out.append("")
        out.append("---")
        out.append("")

    while out and out[-1] in ("", "---"):
        out.pop()
    out.append("")

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
    return len(rows)


# --------------------------------------------------------------------------- #
# Slice rendering — named, filtered views (e.g. EMEA Junior SDE) as a single
# date-sorted table per file. Driven by slices.yaml.
# --------------------------------------------------------------------------- #


def _parse_iso(s: str | None) -> datetime | None:
    """Parse an ISO8601 string into an aware UTC datetime, or None on failure."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _first_seen_sort_key(r: dict) -> str:
    """Strict first_seen DESC ordering (used by slice views).

    Differs from the JOBS.md `_sort_key` which falls back to date_posted —
    slice files explicitly want pipeline-first-sighting order, so a stale
    `date_posted` from a long-stale source doesn't outrank a freshly-found
    row.
    """
    return r.get("first_seen") or ""


def _matches_slice_filters(r: dict, sfilters: dict) -> bool:
    """Apply a slice's filter block (regions / title keywords / kinds).

    Reuses `_classify_intern_or_newgrad` for the kinds gate so intern
    detection stays consistent with emea-entry-level.md.
    """
    regions = sfilters.get("regions")
    if regions:
        wanted = {(x or "").lower() for x in regions if x}
        if (r.get("region") or "").lower() not in wanted:
            return False

    title = (r.get("title") or "").lower()
    any_kws = [(k or "").lower() for k in (sfilters.get("title_keywords_any") or []) if k]
    if any_kws and not any(tok in title for tok in any_kws):
        return False
    none_kws = [(k or "").lower() for k in (sfilters.get("title_keywords_none") or []) if k]
    if none_kws and any(tok in title for tok in none_kws):
        return False

    kinds = sfilters.get("kinds")
    if kinds:
        wanted_kinds = {(k or "").lower() for k in kinds if k}
        if _classify_intern_or_newgrad(r) not in wanted_kinds:
            return False

    return True


def _slice_marker_token(name: str) -> str:
    """Convert a slice name into a safe HTML-comment marker token."""
    return re.sub(r"[^A-Za-z0-9]+", "_", name or "slice").strip("_").upper() or "SLICE"


def render_slice(
    rows: Iterable[dict],
    slice_def: dict,
    output_path: str | Path,
) -> dict[str, int | str | None]:
    """Render one slice markdown file as a single comprehensive table.

    Stats keys returned:
      - total:               deduped row count written to the file
      - last_liveness_sweep: max(liveness_checked_at) across the slice's
                             rendered rows, ISO8601 string. None if no
                             row has been checked yet.
      - pct_verified:        round(100 * ok / total). 0 when total == 0.

    Sort is first_seen DESC; the Age column carries freshness so there
    are no separate 24h / 7d sub-sections.
    """
    name = slice_def.get("name") or "slice"
    title = slice_def.get("title") or name
    sfilters = slice_def.get("filters") or {}

    matching = [r for r in rows if _matches_slice_filters(r, sfilters)]
    deduped = _dedupe_and_sort(matching, _first_seen_sort_key)

    has_salary = any(_has_salary(r) for r in deduped)
    marker_token = _slice_marker_token(name)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out: list[str] = []
    out.append(f"# {title}")
    out.append("")
    out.append(f"Last updated: **{now_str}** · **{len(deduped)}** active roles.")
    out.append("")
    out.append(f"<!-- TABLE_SLICE_{marker_token}_START -->")
    out.append(_render_section(deduped, has_salary))
    out.append(f"<!-- TABLE_SLICE_{marker_token}_END -->")
    out.append("")

    # Liveness summary for INDEX.md — `last_liveness_sweep` is the most
    # recent check across this slice's rows (ISO8601 string; sortable
    # lexically), `pct_verified` is round(100 * ok / total).
    last_sweep: str | None = None
    n_live = 0
    for r in deduped:
        at = r.get("liveness_checked_at")
        if at and (last_sweep is None or str(at) > last_sweep):
            last_sweep = str(at)
        if _is_live(r):
            n_live += 1
    pct_verified = round(100 * n_live / len(deduped)) if deduped else 0

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
    return {
        "total": len(deduped),
        "last_liveness_sweep": last_sweep,
        "pct_verified": pct_verified,
    }


def render_slices(
    active_rows: Iterable[dict],
    slices_config: dict | list,
    output_dir: str | Path,
) -> dict[str, dict[str, int | str | None]]:
    """Render every slice defined in `slices_config` into `output_dir`.

    `slices_config` accepts either the full parsed slices.yaml (dict with
    a top-level `slices:` key) or a bare list of slice definitions.

    Each slice writes one file named by its `filename` field (default
    `<name>.md`). Returns `{slice_name: {total, last_liveness_sweep,
    pct_verified}}` so the caller can build a summary line at end of run
    and feed counts into INDEX.md without re-running the filter/dedup pass.

    Additive to render_md / render_emea_entry_level — slice files are a
    parallel surface, not a replacement.
    """
    if isinstance(slices_config, dict):
        slices = slices_config.get("slices") or []
    elif isinstance(slices_config, list):
        slices = slices_config
    else:
        slices = []

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Filter once at the slice-set level — every slice draws from the same
    # liveness-filtered pool, so an old 404 doesn't sneak back through one
    # slice that happens not to re-filter.
    rows = _filter_liveness(active_rows)

    stats: dict[str, dict[str, int]] = {}
    for s in slices:
        if not isinstance(s, dict):
            continue
        name = (s.get("name") or "").strip()
        if not name:
            continue
        filename = s.get("filename") or f"{name}.md"
        stats[name] = render_slice(rows, s, out_dir / filename)
    return stats


# --------------------------------------------------------------------------- #
# INDEX.md — generated landing page that links every slice with current counts.
# Distinct from README.md, which is the hand-maintained project landing page.
# --------------------------------------------------------------------------- #


# Display order + heading text for grouping slices in INDEX.md. A slice's
# "primary region" is the first entry in its `filters.regions`; anything
# unrecognized (or missing) falls into "other".
_INDEX_REGION_GROUPS: list[tuple[str, str]] = [
    ("emea", "EMEA"),
    ("north_america", "North America"),
    ("other", "Other"),
]


def _fmt_liveness_sweep(iso: str | None) -> str | None:
    """Format an ISO8601 sweep timestamp for INDEX.md (YYYY-MM-DD HH:MM UTC).

    Returns None for falsy input so the caller can skip the line entirely
    when no row has been checked yet.
    """
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except ValueError:
        return str(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def render_index(
    slices_config: dict | list,
    slices_stats: dict[str, dict[str, int | str | None]],
    output_path: str | Path,
    broader_emea_count: int | None = None,
) -> None:
    """Render INDEX.md — table of contents for the rendered files.

    Groups slices by their primary `regions` entry, preserves the order
    given in slices.yaml within each group, and links each entry with its
    `{total} active` count. Slices missing from `slices_stats` (e.g.
    malformed config rows) are silently skipped.

    `broader_emea_count` is the number of rows in `emea-entry-level.md`
    — passed in by the caller so INDEX.md can advertise the wider view.
    """
    if isinstance(slices_config, dict):
        slices = slices_config.get("slices") or []
    elif isinstance(slices_config, list):
        slices = slices_config
    else:
        slices = []

    grouped: dict[str, list[dict]] = {key: [] for key, _ in _INDEX_REGION_GROUPS}
    for s in slices:
        if not isinstance(s, dict):
            continue
        name = (s.get("name") or "").strip()
        if not name or name not in slices_stats:
            continue
        regions = (s.get("filters") or {}).get("regions") or []
        primary = (regions[0] if regions else "other").lower()
        if primary not in grouped:
            primary = "other"
        grouped[primary].append(s)

    out: list[str] = []
    out.append("# Job Tracker")
    out.append("")
    out.append(
        "Auto-generated tracker of tech roles in EMEA and North America "
        "across junior, senior, and specialised tracks (data analyst, "
        "algorithm, solutions / DevOps, quant). Updated twice daily via "
        "GitHub Actions."
    )
    out.append("")
    out.append("## Browse by slice")
    out.append("")

    for region_key, heading in _INDEX_REGION_GROUPS:
        bucket = grouped[region_key]
        if not bucket:
            continue
        out.append(f"### {heading}")
        for s in bucket:
            name = s["name"]
            label = s.get("index_label") or s.get("title") or name
            filename = s.get("filename") or f"{name}.md"
            st = slices_stats[name]
            total = st.get("total", 0) or 0
            out.append(f"- [{label}]({filename}) — {total} active")
            sweep_str = _fmt_liveness_sweep(st.get("last_liveness_sweep"))
            if sweep_str and total:
                pct = st.get("pct_verified", 0) or 0
                out.append(
                    f"  - Last liveness sweep: {sweep_str}, {pct}% verified live"
                )
        out.append("")

    if broader_emea_count is not None:
        out.append("## Wider browse (no curated company allowlist)")
        out.append("")
        out.append(
            f"- [EMEA entry-level (all companies)](emea-entry-level.md) — "
            f"{broader_emea_count} active roles, allowlist gate dropped"
        )
        out.append("")

    out.append("## Coverage")
    out.append("")
    out.append(
        "- Sources: Indeed (CI + local), LinkedIn / Glassdoor / Bayt "
        "(local only), SimplifyJobs/New-Grad-Positions, "
        "SimplifyJobs/Summer2026-Internships"
    )
    out.append(
        "- Cities: London, Dublin, Amsterdam, Berlin, Munich, Zurich, "
        "Paris, Stockholm, Madrid, Barcelona, Vienna, Edinburgh, "
        "Cambridge UK, Manchester"
    )
    out.append("- Refresh: 07:00 UTC, 15:00 UTC")
    out.append("")

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
