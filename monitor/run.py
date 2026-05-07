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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from monitor import db as dbmod
from monitor import notify
from monitor import render_md as render_md_mod


log = logging.getLogger("monitor")


# --------------------------------------------------------------------------- #
# Environment-driven knobs (LinkedIn anti-block tooling)
# --------------------------------------------------------------------------- #


def _proxies_from_env() -> list[str] | None:
    """Read JOBSPY_PROXIES (comma-separated) and pass to JobSpy.

    Format mirrors JobSpy's own `proxies=[...]` param: each item can be
    "user:pass@host:port", "host:port", or "localhost". JobSpy round-robins
    through the list per site. Set this when LinkedIn / Indeed start
    returning 0 rows from a CI runner whose IP got rate-limited.
    Returns None when unset so JobSpy uses its default direct connection.
    """
    raw = os.environ.get("JOBSPY_PROXIES", "").strip()
    if not raw:
        return None
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    return proxies or None


def _linkedin_delay_seconds() -> float:
    """Inter-call sleep after each LinkedIn search.

    Spreads request density to dodge LinkedIn's burst-rate detection.
    Default 5s. LinkedIn calls per run are roughly 14 cities × 4
    templates × 1 term = 56 calls; 5s × 56 = ~5min added to wall clock.
    """
    raw = os.environ.get("LINKEDIN_PER_SEARCH_DELAY", "5")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 5.0


def _running_in_ci() -> bool:
    """True when we're running under GitHub Actions (or any CI that sets it).

    Used to gate `sites_skip_in_ci` — sites whose origin IPs get blocked
    from data-center ranges (notably LinkedIn) are skipped automatically
    in CI so we don't churn empty results, then run normally on local.
    """
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true" or \
        os.environ.get("CI", "").lower() == "true"

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
    """Cross-product cities × role_templates × sites × search_terms.

    Each emitted search is single-site (`site` is a string, not a list)
    so `run_search` can give each site its own location format /
    google_search_term shape / pacing.

    Per-template config knobs:
      - `sites: [list]` — which JobSpy scrapers to call.
      - `search_terms: [list]` — DEFAULT terms used by sites without
        an explicit override.
      - `site_search_terms: {site: [terms]}` — per-site override. We
        use this to give LinkedIn a single broad term (since LinkedIn
        matches loosely and burns IP reputation fast) while letting
        Indeed iterate a longer list (Indeed matches against the
        description, so more terms = more recall).
      - `sites_skip_in_ci: [list]` — sites to drop when `GITHUB_ACTIONS`
        / `CI` env var is set. LinkedIn is the canonical entry here:
        GitHub Actions IPs get blocked, so we run it only on local.

    Glassdoor TLD support is checked per-city via `_glassdoor_supported`;
    cities where Glassdoor has no TLD silently drop it.
    """
    searches: list[dict] = []
    in_ci = _running_in_ci()
    if in_ci:
        log.info("CI mode detected (GITHUB_ACTIONS / CI env set) — applying sites_skip_in_ci")

    for city in cfg["cities"]:
        for tpl in cfg["role_templates"]:
            sites = list(tpl.get("sites") or [])

            if "glassdoor" in sites and not _glassdoor_supported(city["country_indeed"]):
                log.info(
                    "skipping glassdoor for %s (%s) — no Glassdoor TLD",
                    city["name"], city["country_indeed"],
                )
                sites = [s for s in sites if s != "glassdoor"]

            if in_ci:
                ci_skip = set(tpl.get("sites_skip_in_ci") or [])
                drops = [s for s in sites if s in ci_skip]
                if drops:
                    log.info(
                        "CI: dropping %s for %s_%s",
                        drops, tpl["name"], city["name"],
                    )
                sites = [s for s in sites if s not in ci_skip]

            if not sites:
                continue

            default_terms = list(tpl.get("search_terms") or [])
            site_terms_map = tpl.get("site_search_terms") or {}

            for site in sites:
                terms = site_terms_map.get(site) or default_terms
                if not terms:
                    log.warning(
                        "no search_terms for site=%s in template %s — skipping",
                        site, tpl["name"],
                    )
                    continue
                for term in terms:
                    searches.append(
                        {
                            "name": f"{tpl['name']}_{city['name']}",
                            "location": city["location"],
                            "country_indeed": city["country_indeed"],
                            "site": site,
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
    """Call JobSpy for one (site, term, location) tuple. Return list of dicts.

    Per-site quirks handled here (kept centralised so the main loop stays
    flat and `expand_searches` stays a pure cross-product):

      - **glassdoor**: location resolver hates "City, Country" — we strip
        to the city only ("London, United Kingdom" → "London"). This was
        the root cause of the previous Glassdoor disable in CLAUDE.md.

      - **google**: ignores the regular `search_term` and `location`;
        you must compose `google_search_term` yourself in Google Jobs's
        natural-language form ("X jobs near Y since last N days"). We
        synthesize it from the same fields.

      - **linkedin**: nothing JobSpy-side, but the caller is expected to
        sleep `_linkedin_delay_seconds()` after each LinkedIn return.

    Proxies are read from JOBSPY_PROXIES (comma-separated). Errors are
    logged and swallowed — one bad search shouldn't kill the whole run.
    """
    from jobspy import scrape_jobs

    site = search["site"]
    location = search["location"]

    if site == "glassdoor":
        # Glassdoor's findPopularLocationAjax 400's on full "City, Country"
        # strings — the typeahead only resolves bare city names. Strip
        # everything after the first comma.
        location = location.split(",")[0].strip()

    kwargs: dict[str, Any] = {
        "site_name": [site],
        "search_term": search["search_term"],
        "location": location,
        "results_wanted": search["results_wanted"],
        "hours_old": search["hours_old"],
        "country_indeed": search["country_indeed"],
        "job_type": search.get("job_type"),
        "linkedin_fetch_description": False,
        "description_format": "markdown",
        "verbose": 1,
    }

    if site == "google":
        # Google Jobs ignores everything except `google_search_term`, so
        # we encode location + age window inline. Per JobSpy README FAQ:
        # "Search for google jobs on your browser, copy whatever shows
        # up in the search box". The phrasing below mirrors that natural
        # form and works for English-language hubs.
        days = max(1, int(search["hours_old"]) // 24)
        kwargs["google_search_term"] = (
            f"{search['search_term']} jobs near {search['location']} "
            f"since last {days} days"
        )

    proxies = _proxies_from_env()
    if proxies:
        kwargs["proxies"] = proxies

    try:
        df = scrape_jobs(**kwargs)
    except Exception as e:
        log.exception(
            "run_search failed [%s | site=%s | term=%r]: %s",
            search["name"], site, search["search_term"], e,
        )
        return []

    if df is None or df.empty:
        if site == "linkedin":
            log.warning(
                "LinkedIn returned 0 rows for %s/%r — possible IP block. "
                "Set JOBSPY_PROXIES or run locally to recover.",
                search["name"], search["search_term"],
            )
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


def apply_filters(
    rows: list[dict],
    filters: dict,
    skip: set[str] | None = None,
) -> list[dict]:
    """Apply title-exclusion / title-include / company-allow / company-block /
    min-desc filters.

    Title matching uses lowercase + a SPACE-PADDED title (` <title> `), so
    YAML tokens that include surrounding spaces (e.g. ` sr `, ` ii`) act
    as poor-man's word boundaries; bare tokens (`senior`, `staff`) keep
    behaving as plain substrings. We deliberately don't strip the YAML
    tokens — the user uses leading/trailing whitespace as the boundary
    signal.

    `include_companies_mode` controls how `include_companies` is used:
      - "enforce" (default): drop rows whose company isn't on the list.
      - "off": ignore the list entirely (keeps the curated values around so
        you can flip back later without re-typing them).

    `skip` lets a caller turn off individual filter blocks for one batch
    of rows — used by external sources that come pre-curated. Recognised
    names: `exclude_titles`, `include_title_keywords`, `include_companies`,
    `exclude_companies`, `min_description_chars`. Unknown names are
    silently ignored so a typo doesn't crash the run.
    """
    skip = skip or set()
    # Lowercase only — preserve user-authored spaces in tokens.
    excl_titles = [(t or "").lower() for t in filters.get("exclude_titles") or []]
    incl_kws = [(t or "").lower() for t in filters.get("include_title_keywords") or []]
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
        title_padded = " " + _normalize(r.get("title")) + " "
        if "exclude_titles" not in skip and any(
            tok and tok in title_padded for tok in excl_titles
        ):
            continue
        if (
            "include_title_keywords" not in skip
            and incl_kws
            and not any(kw and kw in title_padded for kw in incl_kws)
        ):
            continue

        company = r.get("company") or ""
        if (
            "include_companies" not in skip
            and incl_mode == "enforce"
            and incl_companies
            and not _match_company(company, incl_companies)
        ):
            continue
        if (
            "exclude_companies" not in skip
            and excl_companies
            and _match_company(company, excl_companies)
        ):
            continue

        desc = r.get("description") or ""
        if (
            "min_description_chars" not in skip
            and min_desc
            and len(desc) < min_desc
        ):
            continue

        out.append(r)
    return out


# --------------------------------------------------------------------------- #
# External sources (SimplifyJobs etc.)
# --------------------------------------------------------------------------- #


def ingest_external_sources(
    cfg: dict, conn, run_started_at: str
) -> tuple[int, int]:
    """Pull rows from each entry in cfg['external_sources'], filter, upsert.

    Dispatches by the `type` field. Currently supported types:
      - `simplify_newgrad`  → SimplifyJobs/New-Grad-Positions listings.json
      - `simplify_intern`   → SimplifyJobs/Summer2026-Internships listings.json

    Both share the SimplifyJobs schema and the same fetch/map module.
    Region is auto-classified per listing (a London role lands in EMEA
    regardless of repo). `allowed_regions` (set) lets a source ingest
    only certain regions — typical config: `[emea, north_america]` to
    drop APAC entirely.

    `skip_filters` lets a source opt out of individual filter blocks
    (e.g. `include_companies` because SimplifyJobs is curated and we
    don't want our EMEA-tuned allowlist to gate it).

    Returns (total_filtered_in, total_new). Errors per-source are
    logged and swallowed.
    """
    sources = cfg.get("external_sources") or []
    total_filtered = 0
    total_new = 0
    for src in sources:
        name = (src.get("name") or "").strip()
        # `type` is the dispatch key; falls back to `name` for backward
        # compat with the original single-source config.
        kind = (src.get("type") or src.get("name") or "").strip().lower()
        # Convert "simplify" (legacy) → "simplify_newgrad"
        if kind == "simplify":
            kind = "simplify_newgrad"
        skip = set(src.get("skip_filters") or [])
        allowed = src.get("allowed_regions")
        if allowed is not None:
            allowed = set(allowed)
        default_region = src.get("default_region") or "north_america"

        try:
            if kind in ("simplify_newgrad", "simplify_intern"):
                from monitor.external import simplify as simplify_mod

                url = src.get("url") or simplify_mod.SIMPLIFY_PRESETS.get(kind)
                if not url:
                    log.warning("source %r missing url; skipping", name)
                    continue
                listings = simplify_mod.fetch_listings(url)
                rows = simplify_mod.to_rows(
                    listings,
                    site_label=kind,
                    default_region=default_region,
                    allowed_regions=allowed,
                )
            else:
                log.warning(
                    "unknown external source type %r (name=%r) — skipping",
                    kind, name,
                )
                continue
        except Exception:
            log.exception("ingest %s failed; skipping", name or kind)
            continue

        log.info(
            "external %s (type=%s) | %d rows after region filter",
            name or kind, kind, len(rows),
        )
        filtered = apply_filters(rows, cfg["filters"], skip=skip)
        # Per-region row counts so we can see EMEA contribution at a glance
        by_region: dict[str, int] = {}
        for r in filtered:
            by_region[r.get("region") or "?"] = by_region.get(
                r.get("region") or "?", 0
            ) + 1
        log.info(
            "  %d rows passed filters (skip=%s) → by region: %s",
            len(filtered),
            sorted(skip) or "—",
            ", ".join(f"{k}={v}" for k, v in sorted(by_region.items())),
        )
        total_filtered += len(filtered)
        scraped, new = dbmod.upsert_jobs(
            conn, filtered, run_started_at, f"external:{name or kind}"
        )
        dbmod.record_run(
            conn, run_started_at, f"external:{name or kind}", scraped, new
        )
        total_new += new
    return total_filtered, total_new


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
        linkedin_delay = _linkedin_delay_seconds()
        for search in searches:
            log.info(
                "search %s | site=%s | term=%r | loc=%s",
                search["name"], search["site"],
                search["search_term"], search["location"],
            )
            rows = run_search(search)
            log.info("  scraped %d raw rows", len(rows))
            filtered = apply_filters(rows, cfg["filters"])
            # JobSpy is our EMEA pipeline. Tag rows so the renderer can
            # split them from external (north_america) sources.
            for r in filtered:
                r.setdefault("region", "emea")
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

            # Pace LinkedIn — burst-rate detection is the main reason
            # LinkedIn returns 0 rows from a fresh-IP run. Sleep AFTER
            # each LinkedIn call so the next one isn't back-to-back.
            if search["site"] == "linkedin" and linkedin_delay > 0:
                time.sleep(linkedin_delay)

        # External sources (SimplifyJobs etc.) — runs after the JobSpy
        # loop so mark_gone treats both feeds uniformly.
        ext_filtered, ext_new = ingest_external_sources(
            cfg, conn, run_started_at
        )
        total_filtered_in += ext_filtered
        total_new += ext_new

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
