"""Render JOBS.md from the active rows in jobs.db.

The output mirrors the layout of public new-grad-tracker repos: a header with
section anchors, then three tables — FAANG+ & AI Labs / Quant & Finance /
Other — each grouping the active jobs whose company falls into that tier.

Tier classification is deliberately hardcoded here rather than configurable:
the tiers are a presentation concern and shouldn't bloat config.yaml. If you
want a fourth bucket, add a constant + classify branch below.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


# Tier membership is matched as case-insensitive substring against the company
# name. We don't reuse `_match_company` from run.py because the goal here is
# different — at render time we just need a quick bucket label, not a strict
# allowlist gate. A row with company "Google LLC" matches `google` here.
_FAANG_TOKENS = (
    "google", "meta", "facebook", "amazon", "aws", "microsoft", "apple",
    "netflix", "nvidia", "openai", "anthropic", "deepmind", "mistral",
    "hugging face", "cohere", "stability", "perplexity",
    "oracle", "ibm", "intel", "amd", "salesforce", "adobe", "snap",
    "tiktok", "bytedance", "uber", "airbnb", "spotify", "stripe",
    "databricks", "snowflake", "datadog", "cloudflare", "atlassian",
    "linkedin", "palantir",
)

_QUANT_TOKENS = (
    "bloomberg", "two sigma", "jane street", "hudson river", "citadel",
    "shaw", "millennium", "point72", "optiver", "imc", "flow traders",
    "drw", "maven", "five rings",
    "goldman sachs", "jpmorgan", "jp morgan", "morgan stanley",
    "blackrock", "barclays", "hsbc", "deutsche bank", "ubs",
    "bnp paribas", "santander", "ing bank", "nordea", "credit suisse",
    "bank of america", "merrill lynch",
)


# External "Apply" button image, mirroring the SimplifyJobs new-grad repo.
# If imgur ever rots we just see broken images — links still work.
_APPLY_IMG = (
    '<img src="https://i.imgur.com/JpkfjIq.png" alt="Apply" width="70"/>'
)


def classify(company: str) -> str:
    c = (company or "").lower()
    if not c:
        return "other"
    for tok in _FAANG_TOKENS:
        if tok in c:
            return "faang"
    for tok in _QUANT_TOKENS:
        if tok in c:
            return "quant"
    return "other"


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
        apply_cell = f'<a href="{url}">{_APPLY_IMG}</a>'

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
    return "\n".join(lines)


_TIER_ORDER: list[tuple[str, str]] = [
    ("faang", "FAANG+ & AI Labs"),
    ("quant", "Quant & Finance"),
    ("other", "Other"),
]

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
# SimplifyJobs entries usually point at the company's direct apply URL
# (jobs.apple.com, careers.microsoft.com); JobSpy/Indeed entries are
# referrals through indeed.com / linkedin.com — direct is better.
_SOURCE_PRIORITY = [
    "simplify_newgrad",
    "simplify_intern",
    "simplify",       # legacy label, kept for old DB rows
    "linkedin",
    "indeed",
    "glassdoor",
    "google",
]


def _source_rank(site: str | None) -> int:
    if not site:
        return len(_SOURCE_PRIORITY) + 1
    s = site.lower()
    for i, name in enumerate(_SOURCE_PRIORITY):
        if s == name:
            return i
    return len(_SOURCE_PRIORITY)


def _dedupe_by_signature(rows: list[dict]) -> tuple[list[dict], int]:
    """Collapse rows that share a signature, keep the higher-priority source.

    Operates on a single tier within a region — we never collapse across
    regions, since "Apple, Cupertino" and "Apple, London" are obviously
    different roles even with similar titles.

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


def _split_tiers(region_rows: list[dict]) -> dict[str, list[dict]]:
    by_tier: dict[str, list[dict]] = {k: [] for k, _ in _TIER_ORDER}
    for r in region_rows:
        by_tier[classify(r.get("company") or "")].append(r)
    for tier_key in by_tier:
        # Dedup before sort — sort on the survivors only
        deduped, _collapsed = _dedupe_by_signature(by_tier[tier_key])
        deduped.sort(key=_sort_key, reverse=True)
        by_tier[tier_key] = deduped
    return by_tier


# Title tokens that mark a row as an internship rather than a new-grad role.
# Used by the broader EMEA entry-level view, where SimplifyJobs already tells
# us via `site` (simplify_intern vs simplify_newgrad) but JobSpy rows need to
# be classified from the title itself.
_INTERN_TITLE_TOKENS = (
    "intern",        # "Intern", "Internship", "Software Engineer Intern"
    "placement",     # UK industrial placement
    "year in industry",
    "industrial placement",
    "praktikum",     # German
    "stage",         # French (also Italian)
    "stagiair",      # Dutch
    "becario",       # Spanish
    "tirocinante",   # Italian
    "trainee",       # often (but not always) intern-shaped; close enough
)


def _classify_intern_or_newgrad(r: dict) -> str:
    """Return 'intern' or 'newgrad'. Used only for the EMEA entry-level view.

    SimplifyJobs rows carry the answer in `site` (the upstream repo split is
    intern vs new-grad). For JobSpy rows we substring-match the title; if no
    intern token is present we default to 'newgrad' since the EMEA scrape's
    `job_type=fulltime` filter biases toward new-grad anyway.
    """
    site = (r.get("site") or "").lower()
    if site == "simplify_intern":
        return "intern"
    if site == "simplify_newgrad":
        return "newgrad"
    title = (r.get("title") or "").lower()
    for tok in _INTERN_TITLE_TOKENS:
        if tok in title:
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

    Produces a parallel file (default `emea-entry-level.md`) so the curated
    `JOBS.md` stays small and ntfy-worthy while this file gives the user a
    wide-net browse of every EMEA intern + new-grad role our pipelines saw.
    Layout is Intern / New Grad at the top level, each split into the same
    FAANG / Quant / Other tiers JOBS.md uses.

    Stateless: caller hands in already-fetched rows (region == 'emea',
    title/desc filters applied, `include_companies` skipped). We do not
    touch jobs.db here — net-new alerting is intentionally a JOBS.md-only
    thing so this broader feed doesn't spam ntfy.
    """
    rows = [r for r in rows if (r.get("region") or "").lower() == "emea"]
    has_salary = any(_has_salary(r) for r in rows)

    by_kind: dict[str, list[dict]] = {k: [] for k, _ in _EMEA_KIND_ORDER}
    for r in rows:
        by_kind[_classify_intern_or_newgrad(r)].append(r)

    kind_tiers: dict[str, dict[str, list[dict]]] = {}
    kind_visible: dict[str, int] = {}
    collapsed_total = 0
    for kind_key, _ in _EMEA_KIND_ORDER:
        tiers = _split_tiers(by_kind[kind_key])
        kind_tiers[kind_key] = tiers
        visible = sum(len(t) for t in tiers.values())
        kind_visible[kind_key] = visible
        collapsed_total += len(by_kind[kind_key]) - visible

    visible_total = sum(kind_visible.values())

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append("# EMEA Entry-Level Roles")
    out.append("")
    kind_summaries = [
        f"[{heading}](#{kind_key}) ({kind_visible[kind_key]})"
        for kind_key, heading in _EMEA_KIND_ORDER
    ]
    dedup_note = (
        f" · {collapsed_total} cross-source duplicates merged"
        if collapsed_total
        else ""
    )
    out.append(
        f"Last updated: **{now}** · **{visible_total}** active EMEA "
        f"entry-level roles ({' · '.join(kind_summaries)}){dedup_note}. "
        "Wider net than [JOBS.md](JOBS.md) — drops the curated company "
        "allowlist, keeps title/desc filters. Sources: SimplifyJobs feeds "
        "(intern + new-grad) and the EMEA JobSpy scrape."
    )
    out.append("")
    out.append("---")
    out.append("")

    for kind_key, kind_heading in _EMEA_KIND_ORDER:
        out.append(f'<a name="{kind_key}"></a>')
        out.append(f"## {kind_heading}")
        out.append("")

        tiers = kind_tiers[kind_key]
        toc_bits = [
            f"[{tier_heading}](#{kind_key}-{tier_key}) ({len(tiers[tier_key])})"
            for tier_key, tier_heading in _TIER_ORDER
        ]
        out.append("**Sections:** " + " · ".join(toc_bits))
        out.append("")

        for tier_key, tier_heading in _TIER_ORDER:
            anchor = f"{kind_key}-{tier_key}"
            marker = f"TABLE_EMEA_{kind_key.upper()}_{tier_key.upper()}"
            out.append(f'<a name="{anchor}"></a>')
            out.append(f"### {tier_heading}")
            out.append("")
            out.append(f"<!-- {marker}_START -->")
            out.append(_render_section(tiers[tier_key], has_salary))
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
    """Group `active_rows` into Region × Tier and write the markdown.

    Returns the total row count written. Always overwrites; the file is
    intended to be committed by the workflow alongside jobs.db.

    Layout (single file, EMEA always first since that's the user's
    primary scraper):

        # Junior Tech Roles
        [Last updated, totals, ToC]
        ---
        ## EMEA              (primary)
          ### FAANG+ / Quant / Other  → 3 tables
        ---
        ## North America     (via SimplifyJobs)
          ### FAANG+ / Quant / Other  → 3 tables

    Each table is wrapped in `<!-- TABLE_<REGION>_<TIER>_START -->` /
    `<!-- ..._END -->` HTML markers (mirroring SimplifyJobs / speedyapply)
    so future tooling can do partial-replace on a hand-curated outer file.

    The Salary column is hidden globally whenever no active row carries
    salary data (typical for EMEA Indeed scrapes), keeping every table
    consistently shaped.
    """
    rows = list(active_rows)
    has_salary = any(_has_salary(r) for r in rows)

    # Group by region first; anything with an unknown region tag falls
    # into EMEA so legacy DB rows render somewhere visible.
    by_region: dict[str, list[dict]] = {k: [] for k, _, _ in _REGION_ORDER}
    for r in rows:
        region = (r.get("region") or "emea").lower()
        if region not in by_region:
            region = "emea"
        by_region[region].append(r)

    region_tiers = {
        region_key: _split_tiers(by_region[region_key])
        for region_key, _, _ in _REGION_ORDER
    }

    # Counts AFTER dedup — what the user actually sees.
    region_visible = {
        region_key: sum(len(t) for t in region_tiers[region_key].values())
        for region_key, _, _ in _REGION_ORDER
    }
    visible_total = sum(region_visible.values())
    collapsed = len(rows) - visible_total

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append("# Junior Tech Roles")
    out.append("")

    region_summaries = []
    for region_key, anchor_prefix, heading in _REGION_ORDER:
        n = region_visible[region_key]
        region_summaries.append(f"[{heading}](#{anchor_prefix}) ({n})")
    dedup_note = (
        f" · {collapsed} cross-source duplicates merged" if collapsed else ""
    )
    out.append(
        f"Last updated: **{now}** · **{visible_total}** active roles "
        f"({' · '.join(region_summaries)}){dedup_note}. Generated from "
        f"`monitor/jobs.db` after the latest scrape — see "
        f"[monitor/](monitor/) for how this works."
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

        tiers = region_tiers[region_key]
        toc_bits = []
        for tier_key, tier_heading in _TIER_ORDER:
            n = len(tiers[tier_key])
            toc_bits.append(
                f"[{tier_heading}](#{anchor_prefix}-{tier_key}) ({n})"
            )
        out.append("**Sections:** " + " · ".join(toc_bits))
        out.append("")

        for tier_key, tier_heading in _TIER_ORDER:
            anchor = f"{anchor_prefix}-{tier_key}"
            marker = f"TABLE_{anchor_prefix.upper()}_{tier_key.upper()}"
            out.append(f'<a name="{anchor}"></a>')
            out.append(f"### {tier_heading}")
            out.append("")
            out.append(f"<!-- {marker}_START -->")
            out.append(_render_section(tiers[tier_key], has_salary))
            out.append(f"<!-- {marker}_END -->")
            out.append("")

        out.append("---")
        out.append("")

    # Drop the trailing `---` separator so the file ends on a clean
    # newline rather than a horizontal rule.
    while out and out[-1] in ("", "---"):
        out.pop()
    out.append("")

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
    return len(rows)
