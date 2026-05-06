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


def _render_section(rows: list[dict]) -> str:
    if not rows:
        return "_No active roles in this category._"

    lines = [
        "| Company | Position | Location | Salary | Posting | Age |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        company = _md_escape_cell(r.get("company") or "—")
        title = _md_escape_cell(r.get("title") or "")
        location = _md_escape_cell(r.get("location") or "")
        url = (r.get("job_url") or "#").strip()
        age = days_since(r.get("first_seen"))
        salary = fmt_salary(
            r.get("min_amount"),
            r.get("max_amount"),
            r.get("currency"),
            r.get("salary_interval"),
        )
        company_cell = f"**{company}**"
        cu = (r.get("company_url") or "").strip()
        if cu:
            company_cell = f'<a href="{cu}"><strong>{company}</strong></a>'
        apply_cell = f'<a href="{url}">{_APPLY_IMG}</a>'
        lines.append(
            f"| {company_cell} | {title} | {location} | {salary} | "
            f"{apply_cell} | {age}d |"
        )
    return "\n".join(lines)


def render_md(active_rows: Iterable[dict], output_path: str | Path) -> int:
    """Group `active_rows` into FAANG+/Quant/Other and write the markdown.

    Returns the total row count written. Always overwrites; the file is
    intended to be committed by the workflow alongside jobs.db.
    """
    rows = list(active_rows)
    by_tier = {"faang": [], "quant": [], "other": []}
    for r in rows:
        by_tier[classify(r.get("company") or "")].append(r)

    # Newest-first within each tier (caller already sorts globally — sort
    # again so a tier-only consumer stays consistent).
    for tier_rows in by_tier.values():
        tier_rows.sort(
            key=lambda r: (r.get("first_seen") or "", r.get("company") or ""),
            reverse=True,
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out: list[str] = []
    out.append("# EMEA Junior Tech Roles")
    out.append("")
    out.append(
        f"Last updated: **{now}** · **{len(rows)}** active roles "
        f"across the EMEA hubs we track. Generated from `monitor/jobs.db` "
        f"after the latest scrape — see [monitor/](monitor/) for how this "
        f"works."
    )
    out.append("")
    out.append(
        "**Sections:** "
        f"[FAANG+ & AI Labs](#faang) ({len(by_tier['faang'])}) · "
        f"[Quant & Finance](#quant) ({len(by_tier['quant'])}) · "
        f"[Other](#other) ({len(by_tier['other'])})"
    )
    out.append("")
    out.append("---")
    out.append("")

    out.append('<a name="faang"></a>')
    out.append("## FAANG+ & AI Labs")
    out.append("")
    out.append(_render_section(by_tier["faang"]))
    out.append("")

    out.append('<a name="quant"></a>')
    out.append("## Quant & Finance")
    out.append("")
    out.append(_render_section(by_tier["quant"]))
    out.append("")

    out.append('<a name="other"></a>')
    out.append("## Other")
    out.append("")
    out.append(_render_section(by_tier["other"]))
    out.append("")

    Path(output_path).write_text("\n".join(out), encoding="utf-8")
    return len(rows)
