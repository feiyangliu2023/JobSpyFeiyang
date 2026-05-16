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
    # speedyapply renders FROM SimplifyJobs, so ranking it below means the
    # raw upstream wins the dedup tie; speedyapply rows survive only when
    # they represent postings the canonical feed hasn't picked up yet.
    "speedyapply_newgrad_usa",
    "speedyapply_intern_usa",
    "speedyapply_newgrad_intl",
    "speedyapply_intern_intl",
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


# Render-time caps on the SHAPE of MD output. Bounds the size of every
# rendered section so JOBS.md / slice files / emea-entry-level.md stay
# usable as the broader pool grows. Affects what gets written, NOT what
# gets stored — the full row set stays in jobs.db until the normal
# `prune_old` lifecycle (gone-rows past `retention_days`) catches it.
#
# Defaults are deliberately loose ("loose" was the user's pick): only the
# worst-offender slices (na-junior-sde, na-internships) hit the row cap;
# the age cut hides postings that have been re-listed for half a year.
RENDER_MAX_ROWS = 500
RENDER_MAX_AGE_DAYS = 180


def _apply_render_caps(
    rows: list[dict],
    max_rows: int = RENDER_MAX_ROWS,
    max_age_days: int = RENDER_MAX_AGE_DAYS,
) -> tuple[list[dict], int, int]:
    """Apply (age, count) caps to a sorted row list. Pure / no I/O.

    Assumes `rows` is already sorted newest-first — we drop the tail when
    the count cap kicks in. Pass `max_rows=0` or `max_age_days=0` (or any
    non-positive) to disable that side of the cap.

    Returns `(visible, n_dropped_age, n_dropped_overflow)`. Callers stitch
    the dropped counts into a small footer line via `_render_cap_note`.
    """
    n_dropped_age = 0
    if max_age_days and max_age_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        kept: list[dict] = []
        for r in rows:
            ts = _parse_iso(r.get("first_seen"))
            # Rows without a parseable first_seen are kept; we'd rather
            # over-render than silently drop legacy rows missing the field.
            if ts is None or ts >= cutoff:
                kept.append(r)
            else:
                n_dropped_age += 1
        rows = kept

    n_dropped_overflow = 0
    if max_rows and max_rows > 0 and len(rows) > max_rows:
        n_dropped_overflow = len(rows) - max_rows
        rows = rows[:max_rows]

    return rows, n_dropped_age, n_dropped_overflow


def _render_cap_note(
    n_visible: int,
    n_dropped_age: int,
    n_dropped_overflow: int,
    max_rows: int = RENDER_MAX_ROWS,
    max_age_days: int = RENDER_MAX_AGE_DAYS,
) -> str | None:
    """Build the italicised footer line that explains a render cap.

    Returns None when nothing was dropped (so the caller skips the
    `out.append(...)` entirely and the table looks the same as before).
    """
    if not n_dropped_age and not n_dropped_overflow:
        return None
    parts: list[str] = []
    if n_dropped_overflow:
        total = n_visible + n_dropped_overflow
        parts.append(f"showing newest {n_visible} of {total} active rows (cap {max_rows})")
    if n_dropped_age:
        parts.append(f"{n_dropped_age} hidden as first_seen >{max_age_days}d")
    return f"_{'; '.join(parts)}. Full set in jobs.db._"


# Title-shape tokens that mark a row as an internship. Each entry is a
# regex matched against the lowercased title. We use regex (not bare
# substring) because the obvious naive match — `"intern" in title` —
# also fires on `internal`, `international`, `internet`, which lands
# senior / lead / SRE roles in the intern bucket of entry-level views.
# Pattern shapes:
#   - `intern` — `\bintern\w*\b` (matches Intern, Interns, Internship,
#     Interning — but NOT Internal / International / Internet, which
#     have an `a` / `e` immediately after `intern`).
#   - `placement`, `praktikum`, `stagiair`, `becario`, `tirocinante`,
#     `trainee` — bare word-boundary match.
#   - `year in industry`, `industrial placement` — multi-word phrases,
#     no word-boundary surprise (these don't substring into anything
#     unrelated in practice).
#   - `stage` (FR/IT for internship) — `(?<![\w-])stage(?![\w-])` so
#     "Backstage Platform Engineer" and "Multi-stage Pipeline Engineer"
#     don't false-match. Bare `\bstage\b` isn't enough because `\b`
#     matches at hyphen boundaries.
_INTERN_TITLE_PATTERNS = (
    # `intern` plus only the allowed suffixes (`s`, `ship`, `ships`,
    # `ing`). `\bintern\w*\b` was wrong — `\w*` is greedy and `\b` only
    # marks the END of a word, so `Internal` / `International` /
    # `Internet` would match. Explicit suffix list rules them out.
    re.compile(r"\bintern(?:s|ship|ships|ing)?\b"),
    re.compile(r"\bplacement\b"),
    re.compile(r"\byear in industry\b"),
    re.compile(r"\bindustrial placement\b"),
    re.compile(r"\bpraktikum\b"),
    re.compile(r"\bstagiaire?\b"),
    re.compile(r"\bbecari[oa]\b"),
    re.compile(r"\btirocinante\b"),
    re.compile(r"\btrainee\b"),
    re.compile(r"(?<![\w-])stage(?![\w-])"),
)


def _title_has_intern_marker(title: str) -> bool:
    """True iff the lowercased title contains an intern-shape token."""
    return any(p.search(title) for p in _INTERN_TITLE_PATTERNS)


def _classify_intern_or_newgrad(r: dict) -> str:
    """Return 'intern' or 'newgrad'. Used by the entry-level views.

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
    if _title_has_intern_marker(title):
        return "intern"
    return "newgrad"


# Tokens that mark a title as software/tech-shape — used by the
# entry-level views to drop role families that aren't the audience for
# JOBS.md (Vehicle Testing Engineer Internship, Process Engineering
# Intern, etc.). Substring-matched against lowercased title. Multi-word
# tokens cover the common "qualifier + role" pairings so we don't need
# separate "junior X" / "X intern" entries; "software engineer" already
# matches "Junior Software Engineer" and "Software Engineer Intern".
_TECH_SHAPE_TITLE_TOKENS = (
    "software engineer",
    "software developer",
    "software development",
    "software design",
    "backend",
    "back-end",
    "back end",
    "frontend",
    "front-end",
    "front end",
    "fullstack",
    "full-stack",
    "full stack",
    "web developer",
    "web engineer",
    "mobile engineer",
    "mobile developer",
    "ios engineer",
    "ios developer",
    "android engineer",
    "android developer",
    "platform engineer",
    "infrastructure engineer",
    "systems engineer",
    "security engineer",
    "cloud engineer",
    "cybersecurity",
    "information security",
    "devops",
    "site reliability",
    "machine learning",
    "ml engineer",
    "ml scientist",
    "applied scientist",
    "ai engineer",
    "ai/ml",
    "ai researcher",
    "ai scientist",
    "data scientist",
    "data analyst",
    "data engineer",
    "data analytics",
    "data science",
    "analytics engineer",
    "business intelligence",
    "bi developer",
    "bi engineer",
    "research engineer",
    "research scientist",
    "algorithm",
    "computer vision",
    "nlp engineer",
    "nlp scientist",
    "application developer",
    "applications engineer",
    "application engineer",
    "technical staff",         # MTS at Anthropic / OpenAI / etc.
    "qa engineer",
    "quality engineer",
    "test engineer",
    "automation engineer",
    "release engineer",
    "build engineer",
    "embedded engineer",
    "embedded software",
    "embedded developer",
    "firmware engineer",
    "robotics engineer",
    "robotics software",
    "game developer",
    "game engineer",
    "game programmer",
    "compiler engineer",
    "kernel engineer",
    "performance engineer",
    "production engineer",
    "developer experience",
    "developer advocate",
    "developer relations",
    "growth engineer",
    "search engineer",
    "search scientist",
    "ranking engineer",
    "recommendation",
    "deep learning",
    "reinforcement learning",
)
# Bare initialisms — leading space to avoid mid-word false hits
# (e.g. "INSIDE" → "side"). Trailing token is optional; "SWE" appears
# both as " swe " and " swe," / " swe -" in titles.
_TECH_SHAPE_PATTERNS = (
    re.compile(r"(?<![\w-])(swe|sde|sdet)(?![\w-])"),
    re.compile(r"(?<![\w-])sre(?![\w-])"),
    # `ml` / `ai` standalone — catches "ML Systems", "AI Safety
    # Research", "ML Performance" where the rest of the title doesn't
    # match a longer token. Negative-lookarounds keep "html" / "xml" /
    # "email" / "maintained" / "fail" from false-matching.
    re.compile(r"(?<![\w-])(ml|ai)(?![\w-])"),
)


def _title_has_tech_shape(title: str) -> bool:
    """True iff the lowercased title looks like a software / tech role.

    Drops the "Vehicle Testing Engineer Internship", "Process Engineering
    Intern", "Project Engineer (Graduate)" shape of postings that share
    a region with our target roles but aren't part of the SWE / ML / data
    audience. Matches substring tokens AND a few word-boundary regexes
    for bare SWE/SDE/SRE initialisms.
    """
    if any(tok in title for tok in _TECH_SHAPE_TITLE_TOKENS):
        return True
    return any(p.search(title) for p in _TECH_SHAPE_PATTERNS)


# Tokens that mark a new-grad posting as a batch-hire role (rather than
# a single-headcount specific role). Used by the entry-level new-grad
# section to mirror speedyapply's NEW_GRAD_USA.md shape: big companies
# running named graduate programmes, university hiring tracks, "class
# of 20XX" cohort hires, etc. Substring-matched against lowercased title.
#
# Some entries look loose ("graduate" alone) but are gated by the
# tech-shape filter at the same time, so e.g. "Graduate Project Engineer"
# is still dropped — graduate ✓, tech-shape ✗.
_BATCH_HIRE_TITLE_TOKENS = (
    "new grad",
    "newgrad",
    "new-grad",
    "new graduate",
    "graduate",
    "early career",
    "early-career",
    "early in career",
    "campus",
    "university hire",
    "university graduate",
    "university talent",
    "college hire",
    "college graduate",
    "associate software",
    "associate engineer",
    "associate developer",
    "associate data",
    "junior software",
    "junior developer",
    "junior data",
    "junior ml",
    "junior machine learning",
    "junior ai",
    "junior backend",
    "junior frontend",
    "junior full",
    "junior research",
    "entry level",
    "entry-level",
    "rotational",
    "class of 20",
    "fresh graduate",
    "recent graduate",
    "first job",
    "apprenticeship",
    "apprentice",
    "trainee",
    "scholarship",
    "fellowship",
    "fellows program",
    "residency program",
    "residency",
    "internship program",
)
# "Software Engineer I", "Software Engineer 1", "SDE I" — the suffix
# numeric / Roman "1" / "I" is the entry-level signal at Amazon / MSFT
# / Bloomberg, but it's too short to substring-match without false
# positives. Anchor on the role-shape immediately before.
_BATCH_HIRE_LEVEL_RE = re.compile(
    r"\b(?:engineer|developer|scientist|analyst|sde|swe)\s*[-,]?\s*(?:i|1|l1|level\s*1)\b"
)


def _title_has_batch_hire_marker(title: str) -> bool:
    """True iff the lowercased title carries a batch-hire signal.

    Recognises:
      - explicit phrasings (`new grad`, `graduate`, `campus`, `university
        hire`, `early career`, `entry level`, `class of 20XX`, ...)
      - junior / associate / apprentice / trainee variants
      - level suffixes (`Software Engineer I`, `SWE 1`, `SDE Level 1`)
    """
    if any(tok in title for tok in _BATCH_HIRE_TITLE_TOKENS):
        return True
    return bool(_BATCH_HIRE_LEVEL_RE.search(title))


# Seniority tokens that disqualify a row from the entry-level views.
# Substring-matched against lowercased title. We don't reuse the broader
# `exclude_titles` from config.yaml here because that list also strips
# tokens we WANT in entry-level (e.g. `internship` is technically an
# excludable specifier for senior search-term recall but obviously must
# stay for intern roles).
_ENTRY_LEVEL_SENIORITY_DROP_TOKENS = (
    "senior",
    " sr.",
    " sr ",
    "staff",
    "principal",
    " lead ",
    "lead engineer",
    "lead software",
    "lead developer",
    "lead data",
    "lead ml",
    "lead ai",
    "lead research",
    "team lead",
    "tech lead",
    "manager",
    "director",
    "head of",
    "vice president",
    "vp ",
    # Academic / non-batch roles — universities don't run batch SWE
    # intake under these titles. Caught here because tech-shape can
    # over-match ("Lecturer in Software Engineering" contains "software
    # engineer" as a substring).
    "lecturer",
    "professor",
    "postdoc",
    "post-doc",
    "postdoctoral",
)


def _row_is_curated_source(r: dict) -> bool:
    """True iff the row came from a human-curated upstream feed (where
    inclusion in the repo IS the new-grad / intern signal).

    Includes SimplifyJobs / vanshb03 / similar tracker repos.

    Excludes `direct:*` ATS scrapers — those pull every open role from
    the company's career site without seniority curation. Companies
    like Anthropic / OpenAI / Cohere list senior IC roles alongside
    occasional new-grad programmes, and the title is the only signal
    we get. Speedyapply's NEW_GRAD_*.md is a pure render of the
    SimplifyJobs feed, so honouring SimplifyJobs's upstream curation
    is what guarantees parity with that view.
    """
    site = (r.get("site") or "").lower()
    if not site:
        return False
    return (
        "simplify" in site
        or "newgrad" in site
        or "new-grad" in site
        or "new_grad" in site
        or "summer202" in site
        or "vanshb03" in site
        or "speedyapply" in site
    )


def _title_passes_entry_level_filter(r: dict, kind: str) -> bool:
    """Gate for the entry-level views: tech-shape + batch-hire + no senior.

    Args:
      r: row dict (uses `title` and `site`).
      kind: 'intern' or 'newgrad'.

    Two-tier gating, matched to the source's signal strength:
      - Senior / lead / staff / principal — dropped for every row
        regardless of source (these are obvious miscategorisations,
        usually caused by the legacy "intern" substring matching
        "Internal").
      - Tech-shape (Software Engineer, ML, Data, AI, …) — required for
        every row. Drops "Vehicle Testing Engineer Internship",
        "Process Engineering Intern", "Graduate Project Engineer" —
        roles that share keywords with our audience but aren't part
        of the SWE / ML / data target.
      - Batch-hire marker (graduate / new grad / university / campus /
        early career / SWE I, …) — required only for new-grad rows
        from raw JobSpy aggregators (Indeed / LinkedIn / Glassdoor).
        Curated upstream feeds (SimplifyJobs / vanshb03 / `direct:*`)
        skip this gate so we always include at least what speedyapply
        renders.
    """
    title = (r.get("title") or "").lower()
    if not title:
        return False
    if any(tok in title for tok in _ENTRY_LEVEL_SENIORITY_DROP_TOKENS):
        return False
    if not _title_has_tech_shape(title):
        return False
    if kind == "newgrad" and not _row_is_curated_source(r):
        if not _title_has_batch_hire_marker(title):
            return False
    return True


_ENTRY_LEVEL_KIND_ORDER: list[tuple[str, str]] = [
    ("intern", "Internships"),
    ("newgrad", "Full-Time New Grad"),
]


_ENTRY_LEVEL_REGION_HEADINGS = {
    "emea": "EMEA",
    "north_america": "North America",
}


def render_region_entry_level(
    rows: Iterable[dict],
    region: str,
    output_path: str | Path,
) -> int:
    """Broader entry-level view for one region — same row shape as JOBS.md
    but no company allowlist gate, scoped to a single region.

    One comprehensive table per kind (Internships / New Grad). Sorted
    newest-first; the Age column carries freshness, so no separate 24h /
    7d sections.

    Title-level filter (`_title_passes_entry_level_filter`):
      - Drops senior / lead / staff / principal titles.
      - Requires tech-shape (Software / ML / AI / Data / SWE / …).
      - For new-grad rows from raw JobSpy aggregators, also requires a
        batch-hire marker (graduate / new grad / university / campus /
        Software Engineer I / etc.) so single-headcount specific
        postings don't crowd out the graduate-programme intake roles.
        Curated upstream feeds (SimplifyJobs / vanshb03 / `direct:*`)
        skip the marker gate — they're pre-vetted by humans, so passing
        them through unmodified guarantees we cover at least what
        speedyapply's NEW_GRAD_*.md renders.
    """
    region_key = (region or "").lower()
    region_heading = _ENTRY_LEVEL_REGION_HEADINGS.get(region_key, region_key.upper() or "Unknown")

    rows = [r for r in rows if (r.get("region") or "").lower() == region_key]
    rows = _filter_liveness(rows)
    has_salary = any(_has_salary(r) for r in rows)

    by_kind: dict[str, list[dict]] = {k: [] for k, _ in _ENTRY_LEVEL_KIND_ORDER}
    for r in rows:
        kind = _classify_intern_or_newgrad(r)
        if not _title_passes_entry_level_filter(r, kind):
            continue
        by_kind[kind].append(r)

    kind_rows: dict[str, list[dict]] = {}
    for kind_key, _ in _ENTRY_LEVEL_KIND_ORDER:
        kind_rows[kind_key] = _dedupe_and_sort(by_kind[kind_key])

    # Per-kind render caps. Header / kind-summary counts continue to show
    # the true active total; each table is capped to keep the file size
    # bounded as the broader pool grows.
    kind_active = {k: len(kind_rows[k]) for k, _ in _ENTRY_LEVEL_KIND_ORDER}
    kind_caps: dict[str, tuple[int, int]] = {}
    for kind_key, _ in _ENTRY_LEVEL_KIND_ORDER:
        capped, n_age, n_overflow = _apply_render_caps(kind_rows[kind_key])
        kind_rows[kind_key] = capped
        kind_caps[kind_key] = (n_age, n_overflow)

    visible_total = sum(kind_active.values())

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append(f"# {region_heading} Entry-Level Roles")
    out.append("")
    kind_summaries = [
        f"[{heading}](#{kind_key}) ({kind_active[kind_key]})"
        for kind_key, heading in _ENTRY_LEVEL_KIND_ORDER
    ]
    out.append(
        f"Last updated: **{now}** · **{visible_total}** active "
        f"{region_heading} entry-level roles, split into "
        f"{' · '.join(kind_summaries)}."
    )
    out.append("")
    out.append(
        "Split between **Internships** (typically part-time, co-op, or "
        "summer) and **Full-Time New Grad** (entry-level full-time "
        "headcount) so the two pipelines are easy to scan separately."
    )
    out.append("")
    out.append(
        "Filtered to tech-shape titles (software, ML, AI, data, etc.) "
        "from batch-hire intake at big companies — mirrors the "
        "[speedyapply](https://github.com/speedyapply/2026-SWE-College-Jobs) "
        "NEW_GRAD shape. Senior / lead / staff postings excluded."
    )
    out.append("")
    out.append("---")
    out.append("")

    marker_prefix = "TABLE_NA" if region_key == "north_america" else f"TABLE_{region_key.upper()}"
    for kind_key, kind_heading in _ENTRY_LEVEL_KIND_ORDER:
        marker = f"{marker_prefix}_{kind_key.upper()}"
        out.append(f'<a name="{kind_key}"></a>')
        out.append(f"## {kind_heading}")
        out.append("")
        out.append(f"<!-- {marker}_START -->")
        out.append(_render_section(kind_rows[kind_key], has_salary))
        out.append(f"<!-- {marker}_END -->")
        n_age, n_overflow = kind_caps[kind_key]
        cap_note = _render_cap_note(len(kind_rows[kind_key]), n_age, n_overflow)
        if cap_note:
            out.append("")
            out.append(cap_note)
        out.append("")
        out.append("---")
        out.append("")

    while out and out[-1] in ("", "---"):
        out.pop()
    out.append("")

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
    return visible_total


def render_emea_entry_level(
    rows: Iterable[dict], output_path: str | Path
) -> int:
    """Back-compat alias — renders the EMEA entry-level view.

    New callers should use `render_region_entry_level(rows, 'emea', path)`
    directly. Kept so `monitor/run.py` doesn't need to change in step
    with this refactor.
    """
    return render_region_entry_level(rows, "emea", output_path)


def render_na_entry_level(
    rows: Iterable[dict], output_path: str | Path
) -> int:
    """Render the North America entry-level view."""
    return render_region_entry_level(rows, "north_america", output_path)


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

    # Apply render caps PER REGION so JOBS.md stays browsable as the NA
    # pool grows. The header still advertises the true active count
    # (pre-cap); the per-region table shows the visible subset and the
    # footer note explains what was hidden.
    region_active = {k: len(region_rows[k]) for k, _, _ in _REGION_ORDER}
    region_caps: dict[str, tuple[int, int]] = {}
    for region_key, _, _ in _REGION_ORDER:
        capped, n_age, n_overflow = _apply_render_caps(region_rows[region_key])
        region_rows[region_key] = capped
        region_caps[region_key] = (n_age, n_overflow)

    region_visible = {k: len(region_rows[k]) for k, _, _ in _REGION_ORDER}
    visible_total = sum(region_active.values())

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append("# Junior Tech Roles")
    out.append("")

    region_summaries = [
        f"[{heading}](#{anchor_prefix}) ({region_active[region_key]})"
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
        n_age, n_overflow = region_caps[region_key]
        cap_note = _render_cap_note(region_visible[region_key], n_age, n_overflow)
        if cap_note:
            out.append("")
            out.append(cap_note)
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
    # Sort by date_posted DESC (fallback first_seen) — matches the visible
    # `Age` column, so the table reads top-to-bottom newest-to-oldest by
    # the same signal the user reads off each row. Previously this used
    # a strict first_seen-only key, which produced visible disorder when
    # many rows shared a first_seen timestamp (e.g. on a freshly re-seeded
    # DB).
    deduped = _dedupe_and_sort(matching)

    # Per-slice cap overrides — slices.yaml may set max_rows / max_age_days
    # to override the module-level defaults. 0 / None disables that cap.
    slice_max_rows = slice_def.get("max_rows", RENDER_MAX_ROWS)
    slice_max_age = slice_def.get("max_age_days", RENDER_MAX_AGE_DAYS)
    n_total = len(deduped)
    visible, n_age, n_overflow = _apply_render_caps(
        deduped, slice_max_rows, slice_max_age,
    )

    has_salary = any(_has_salary(r) for r in visible)
    marker_token = _slice_marker_token(name)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out: list[str] = []
    out.append(f"# {title}")
    out.append("")
    out.append(f"Last updated: **{now_str}** · **{n_total}** active roles.")
    out.append("")
    out.append(f"<!-- TABLE_SLICE_{marker_token}_START -->")
    out.append(_render_section(visible, has_salary))
    out.append(f"<!-- TABLE_SLICE_{marker_token}_END -->")
    cap_note = _render_cap_note(
        len(visible), n_age, n_overflow, slice_max_rows, slice_max_age,
    )
    if cap_note:
        out.append("")
        out.append(cap_note)
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
    broader_na_count: int | None = None,
) -> None:
    """Render INDEX.md — table of contents for the rendered files.

    Groups slices by their primary `regions` entry, preserves the order
    given in slices.yaml within each group, and links each entry with its
    `{total} active` count. Slices missing from `slices_stats` (e.g.
    malformed config rows) are silently skipped.

    `broader_emea_count` / `broader_na_count` are the row counts in
    `emea-entry-level.md` / `na-entry-level.md` — passed in by the
    caller so INDEX.md can advertise the wider views.
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

    if broader_emea_count is not None or broader_na_count is not None:
        out.append("## Wider browse (no curated company allowlist)")
        out.append("")
        if broader_emea_count is not None:
            out.append(
                f"- [EMEA entry-level (all companies)](emea-entry-level.md) — "
                f"{broader_emea_count} active roles, allowlist gate dropped"
            )
        if broader_na_count is not None:
            out.append(
                f"- [North America entry-level (all companies)](na-entry-level.md) — "
                f"{broader_na_count} active roles, allowlist gate dropped"
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
