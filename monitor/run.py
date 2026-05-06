"""Job monitor entry point.

  python -m monitor.run [--config monitor/config.yaml] [--db monitor/jobs.db]

Reads the YAML spec, expands cities x role_templates x search_terms into
concrete searches, runs each via JobSpy, applies the local filter block, and
upserts results into SQLite. Net-new jobs are pushed to ntfy.sh as a single
digest per run.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from monitor import db as dbmod
from monitor import notify
from monitor import render_md as render_md_mod


log = logging.getLogger("monitor")

# Glassdoor isn't supported for every country. The Country enum's value is a
# 2- or 3-tuple; the 3rd element is the glassdoor TLD spec. If absent, we
# silently drop glassdoor from a search's site list.
_GLASSDOOR_UNAVAILABLE_COUNTRIES_FALLBACK = {"sweden", "norway", "finland", "denmark"}


# --------------------------------------------------------------------------- #
# Config loading + expansion
# --------------------------------------------------------------------------- #


def load_config(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config at {path} did not parse to a dict")
    for required in ("cities", "role_templates", "filters"):
        if required not in cfg:
            raise ValueError(f"Config missing required top-level key: {required}")
    return cfg


def _glassdoor_supported(country_indeed: str) -> bool:
    """Best-effort check: ask the Country enum whether it has a glassdoor TLD."""
    try:
        from jobspy.model import Country

        c = Country.from_string(country_indeed)
        return len(c.value) >= 3
    except Exception:
        return country_indeed.lower() not in _GLASSDOOR_UNAVAILABLE_COUNTRIES_FALLBACK


def expand_searches(cfg: dict) -> list[dict]:
    """Cross-product cities x role_templates x search_terms -> flat search list."""
    searches: list[dict] = []
    for city in cfg["cities"]:
        for tpl in cfg["role_templates"]:
            sites = list(tpl.get("sites") or [])
            if "glassdoor" in sites and not _glassdoor_supported(city["country_indeed"]):
                log.info(
                    "skipping glassdoor for %s (%s) — not supported",
                    city["name"],
                    city["country_indeed"],
                )
                sites = [s for s in sites if s != "glassdoor"]
            if not sites:
                continue
            for term in tpl.get("search_terms") or []:
                searches.append(
                    {
                        "name": f"{tpl['name']}_{city['name']}",
                        "location": city["location"],
                        "country_indeed": city["country_indeed"],
                        "sites": sites,
                        "search_term": term,
                        "results_wanted": int(tpl.get("results_wanted", 30)),
                        "hours_old": int(tpl.get("hours_old", 72)),
                        "job_type": tpl.get("job_type"),
                    }
                )
    return searches


# --------------------------------------------------------------------------- #
# Scraping
# --------------------------------------------------------------------------- #


def run_search(search: dict) -> list[dict]:
    """Call JobSpy for one (term, location, sites) tuple. Return list of dicts.

    Errors are logged and swallowed — one bad search shouldn't kill the run.
    """
    from jobspy import scrape_jobs

    try:
        df = scrape_jobs(
            site_name=search["sites"],
            search_term=search["search_term"],
            location=search["location"],
            results_wanted=search["results_wanted"],
            hours_old=search["hours_old"],
            country_indeed=search["country_indeed"],
            job_type=search.get("job_type"),
            linkedin_fetch_description=False,
            description_format="markdown",
            verbose=1,
        )
    except Exception as e:
        log.exception(
            "run_search failed [%s | %s | %s]: %s",
            search["name"], search["search_term"], search["sites"], e,
        )
        return []

    if df is None or df.empty:
        return []
    return _df_to_dicts(df)


def _df_to_dicts(df: Any) -> list[dict]:
    """Convert a pandas DataFrame to JSON-friendly dicts. NaN -> None."""
    rows: list[dict] = []
    for raw in df.to_dict(orient="records"):
        clean: dict = {}
        for k, v in raw.items():
            if v is None:
                clean[k] = None
            elif isinstance(v, float) and math.isnan(v):
                clean[k] = None
            else:
                clean[k] = v
        # Normalize date_posted to a string if present
        dp = clean.get("date_posted")
        if dp is not None and not isinstance(dp, str):
            try:
                clean["date_posted"] = dp.isoformat()
            except Exception:
                clean["date_posted"] = str(dp)
        rows.append(clean)
    return rows


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #


def _normalize(s: Any) -> str:
    return (s or "").strip().lower()


# Corporate suffixes we strip before matching, so "Google LLC", "Adyen N.V.",
# "Booking.com B.V." all reduce to bare brand names. Iterated until stable
# (handles compound suffixes like "Foo Inc., Ltd."). Order matters only for
# readability — the loop normalizes regardless.
_CORP_SUFFIXES_RE = re.compile(
    r"[,\s]+(?:llc|l\.l\.c\.?|inc\.?|incorporated|limited|ltd\.?|gmbh|ag|"
    r"s\.a\.?|sa|sarl|sas|s\.r\.l\.?|srl|n\.v\.?|nv|b\.v\.?|bv|plc|co\.?|"
    r"corp\.?|corporation|company|se|oyj|ab|aps|pte|pty|holdings?|group)\.?"
    r"\s*$",
    re.IGNORECASE,
)
# Common decorative bits — drop noise like "& Co", trailing punctuation.
_TRIM_RE = re.compile(r"[\s,&\-/]+$")


def _normalize_company_name(name: str) -> str:
    """Reduce a raw company string to its bare brand for word-boundary matching.

    Applies suffix stripping iteratively: a name like "Foo, Inc., Ltd."
    collapses to "foo" rather than only chopping the outermost suffix.
    """
    name = _normalize(name)
    if not name:
        return ""
    # Drop a leading "the "
    if name.startswith("the "):
        name = name[4:]
    # Iteratively strip suffixes (cap at 4 passes — overrunning would just
    # mean the name was unusual, not infinite).
    for _ in range(4):
        new = _CORP_SUFFIXES_RE.sub("", name)
        new = _TRIM_RE.sub("", new).strip()
        if new == name:
            break
        name = new
    return name


def _match_company(company: str, allowlist: list) -> bool:
    """Word-boundary substring match against an allowlist of tokens / synonyms.

    `allowlist` items are either strings (single token) or lists of strings
    (synonyms — any hit counts). The token must appear as a whole word in
    the company name; this prevents "meta" from matching "Metaverse Labs"
    while still matching "Meta Platforms Inc".

    Two candidate forms are tried for every company:
      - raw lowercase (preserves periods so token `booking.com` matches
        "Booking.com B.V.")
      - period-collapsed (`j.p. morgan` -> `jp morgan`, so token `jp morgan`
        matches the dotted form)
    """
    raw = _normalize(company)
    if not raw:
        return False
    if raw.startswith("the "):
        raw = raw[4:]
    candidates = {raw, raw.replace(".", "")}

    for entry in allowlist:
        tokens = entry if isinstance(entry, list) else [entry]
        for tok in tokens:
            tok_norm = _normalize(tok)
            if not tok_norm:
                continue
            # `\b` against tokens that start/end with non-word chars (period in
            # `booking.com`) wouldn't match — anchor on `(?:^|\b)` and
            # `(?:\b|$)` instead, which works for both regular words and
            # punctuated tokens.
            pattern = r"(?:^|\b)" + re.escape(tok_norm) + r"(?:\b|$)"
            for cand in candidates:
                if re.search(pattern, cand):
                    return True
    return False


def apply_filters(rows: list[dict], filters: dict) -> list[dict]:
    """Apply title-exclusion / company-allow / company-block / min-desc filters.

    `include_companies_mode` controls how `include_companies` is used:
      - "enforce" (default): drop rows whose company isn't on the list.
      - "off": ignore the list entirely (keeps the curated values around so
        you can flip back later without re-typing them).
    """
    excl_titles = [_normalize(t) for t in filters.get("exclude_titles") or []]
    incl_companies = filters.get("include_companies") or []
    # YAML 1.1 coerces bare `off`/`no`/`false` to a Python bool, so be liberal
    # in what we accept. Anything truthy/strict-looking → enforce; falsy or
    # "off"/"disabled"/"none" → off.
    raw_mode = filters.get("include_companies_mode")
    if isinstance(raw_mode, bool):
        incl_mode = "enforce" if raw_mode else "off"
    else:
        incl_mode = (raw_mode or "enforce").strip().lower()
        if incl_mode in ("disabled", "none", "false", "no", "0"):
            incl_mode = "off"
    excl_companies = filters.get("exclude_companies") or []
    min_desc = int(filters.get("min_description_chars") or 0)

    out: list[dict] = []
    for r in rows:
        title = _normalize(r.get("title"))
        if any(tok and tok in title for tok in excl_titles):
            continue

        company = r.get("company") or ""
        if (
            incl_mode == "enforce"
            and incl_companies
            and not _match_company(company, incl_companies)
        ):
            continue
        if excl_companies and _match_company(company, excl_companies):
            continue

        desc = r.get("description") or ""
        if min_desc and len(desc) < min_desc:
            continue

        out.append(r)
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "run.log", encoding="utf-8"),
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the job monitor.")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="path to YAML config",
    )
    parser.add_argument(
        "--db",
        default=str(Path(__file__).parent / "jobs.db"),
        help="path to SQLite DB",
    )
    parser.add_argument(
        "--log-dir",
        default=str(Path(__file__).parent / "logs"),
        help="directory for log files",
    )
    parser.add_argument(
        "--md",
        default=str(Path(__file__).parent.parent / "JOBS.md"),
        help="path to the rendered markdown table (committed by CI)",
    )
    args = parser.parse_args(argv)

    _setup_logging(Path(args.log_dir))

    run_started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    log.info("monitor start: run_started_at=%s", run_started_at)

    cfg = load_config(args.config)
    searches = expand_searches(cfg)
    log.info("expanded %d concrete searches", len(searches))

    conn = dbmod.setup_db(args.db)

    total_scraped = 0
    total_filtered_in = 0
    total_new = 0

    try:
        for search in searches:
            log.info(
                "search %s | term=%r | sites=%s | loc=%s",
                search["name"], search["search_term"],
                search["sites"], search["location"],
            )
            rows = run_search(search)
            log.info("  scraped %d raw rows", len(rows))
            filtered = apply_filters(rows, cfg["filters"])
            log.info("  %d rows passed filters", len(filtered))
            total_scraped += len(rows)
            total_filtered_in += len(filtered)
            scraped, new = dbmod.upsert_jobs(
                conn, filtered, run_started_at, search["name"]
            )
            dbmod.record_run(
                conn, run_started_at, search["name"], scraped, new
            )
            total_new += new

        gone = dbmod.mark_gone(conn, run_started_at)
        log.info(
            "done: scraped=%d, filtered=%d, new=%d, marked_gone=%d",
            total_scraped, total_filtered_in, total_new, gone,
        )

        new_jobs = dbmod.fetch_new_since(conn, run_started_at)
        if new_jobs:
            sent = notify.send_digest(new_jobs, topic=os.environ.get("NTFY_TOPIC"))
            log.info("notification sent=%s", sent)
        else:
            log.info("no new jobs; skipping ntfy")

        # Render JOBS.md from the current DB state regardless of whether this
        # run added anything — gone-jobs disappear, ages tick up.
        active = dbmod.fetch_active(conn)
        n_rendered = render_md_mod.render_md(active, args.md)
        log.info("rendered %d active jobs to %s", n_rendered, args.md)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
