# monitor/ — context for future sessions

## Purpose

Twice-daily scheduled scrape of EMEA junior-level SDE and entry-level AI roles
(MLE, Applied Scientist) at large tech companies. Net-new postings (deduped
against `jobs.db`) are pushed as a single digest to ntfy.sh per run.

The user's phone subscribes to an ntfy topic; that's the only delivery channel.

## Library used

This directory sits alongside the upstream JobSpy package (the fork's `jobspy/`
folder at the repo root). We `pip install -e ..` so JobSpy is importable as
`jobspy`. The relevant API:

- `jobspy.scrape_jobs(site_name=[...], search_term=..., location=...,
  results_wanted=N, hours_old=N, country_indeed=..., job_type=...,
  linkedin_fetch_description=False, ...) -> pandas.DataFrame`
- DataFrame columns we care about: `site`, `job_url`, `title`, `company`,
  `location`, `date_posted`, `description`.
- Site values: `linkedin`, `indeed`, `glassdoor`, `google`, `zip_recruiter`,
  `bayt`, `naukri`, `bdjobs`. We use only the first three.
- `country_indeed` accepts country names defined in `jobspy.model.Country`.
  Glassdoor support requires a 3-tuple in the enum value (e.g. Sweden has
  no Glassdoor TLD — see `_glassdoor_supported` in `run.py`).

We keep `monitor/` separate from `jobspy/` so upstream pulls don't conflict.

## Search spec

`monitor/config.yaml`. Concrete searches are generated as the cross-product of
`cities x role_templates x search_terms`. Each (template, city) pair becomes
one logical bucket named `<template>_<city>` (e.g. `sde_junior_london`); the
multiple search_terms within a template all upsert under that same bucket.

Filters are applied in Python after scraping (JobSpy's own filtering is
limited): `exclude_titles` (substring match on title), `include_companies`
(allowlist with synonym lists and suffix-tolerant matching),
`exclude_companies`, and `min_description_chars`.

## DB schema

`monitor/jobs.db` (SQLite, committed to the repo for persistence across runs).

```
jobs(
  job_url        PRIMARY KEY,        -- the dedup key
  site, title, company, company_url, location, is_remote,
  date_posted, description, search_name,
  min_amount, max_amount, currency, salary_interval,  -- nullable; populated when JobSpy returns salary data
  first_seen     TEXT NOT NULL,      -- ISO8601 UTC, set on insert
  last_seen      TEXT NOT NULL,      -- ISO8601 UTC, refreshed on each upsert
  status         TEXT NOT NULL       -- 'active' or 'gone'
)

runs(
  run_id       INTEGER PK,
  started_at, search_name, rows_scraped, rows_new
)
```

"New" = rows whose `first_seen` equals this run's timestamp. After the upsert
loop, any active row whose `last_seen < this_run` is flipped to `'gone'`.

The salary / company_url / is_remote columns were added after the original
schema; `setup_db` runs `ALTER TABLE ADD COLUMN` statements wrapped in
try/except so older DBs upgrade in place.

## JOBS.md (rendered table)

`render_md.render_md(active_rows, path)` writes `JOBS.md` at the repo root
on every run. It's grouped into three tiers — FAANG+ & AI Labs / Quant &
Finance / Other — with anchor links at the top. Tier classification is a
hardcoded substring match in `render_md.py`; it's deliberately separate
from the strict word-boundary `_match_company` allowlist gate, because at
render time we just need a quick bucket label, not a precision filter.

The "Apply" cell embeds an external imgur image (the same one used by
SimplifyJobs/New-Grad-Positions). If imgur rots, the link still works.

## Known issues

- **LinkedIn blocks GitHub Actions IPs aggressively.** Expect sparse or empty
  LinkedIn results from CI. The user accepts this and runs locally as a
  fallback (`python -m monitor.run`). Do NOT add proxy logic to "fix" it.
- **Glassdoor is currently disabled** in `config.yaml` because its
  `findPopularLocationAjax.htm?term=...` endpoint 400s on every "City,
  Country" string (e.g. "London, United Kingdom") — only the city alone
  matches its typeahead. We didn't want to split JobSpy into per-site calls,
  and Indeed already covers the same job surface. To re-enable: split
  `run_search` into one JobSpy call per (location, sites) bucket so
  Glassdoor can receive city-only locations.
- **Glassdoor is unavailable for several countries even when working**
  (Sweden, Norway, etc.) — `expand_searches` keeps a country-list fallback
  in case Glassdoor is reinstated.
- **EMEA postings are often bilingual or non-English.** We rely on the fact
  that seniority terms in titles are usually English even at non-English
  companies. Substring filters work most of the time. Don't try to localize.
- **Some descriptions are very long.** We store them in SQLite but never put
  them in the ntfy body — only title/company/location/link.

## Reasonable choices made (call out before changing)

- `description_format="markdown"` — JobSpy's default.
- `linkedin_fetch_description=False` — fetching descriptions per LinkedIn
  posting roughly 10x's the request count and gets us blocked faster.
- One `record_run` row per (search_name, run) pair — multiple search_terms
  under the same template each get their own row, since they're distinct
  scrapes even if they share a bucket.
- ntfy POST uses the JSON format (sets title, tags, priority cleanly) and
  base URL `https://ntfy.sh` (overridable via `NTFY_BASE_URL` env).
- Notification skipped entirely (no spam) when zero net-new across all
  searches.
- Workflow's commit step **stages first, then checks
  `git diff --cached --quiet`** — `git diff` alone misses untracked files,
  so on the first ever run the new SQLite file would never get committed.
- Allowlist matching is **word-boundary regex on a suffix-stripped form**
  of the company name (see `_normalize_company_name` + `_match_company` in
  run.py). Suffixes stripped: LLC, Inc, GmbH, AG, Ltd, B.V., NV, S.A.,
  SARL, SAS, SE, plc, Corp, Holdings, Group, etc. Iterated up to 4 times
  to handle compound suffixes like "Foo, Inc., Ltd". This deliberately
  prevents "meta" from matching "Metaverse Labs" — the trade-off is that
  brand-new companies whose name has unusual punctuation may slip through;
  add them as a synonym list if it happens.

## Do NOT add without asking

- Proxies / rotating IPs (won't help reliably and adds cost).
- `asyncio` / `aiohttp` (JobSpy already does its own thread pool internally).
- An ORM (SQLAlchemy, Peewee — raw `sqlite3` is plenty for this scale).
- LLM-based filtering (latency, cost, non-determinism for a cron job).
- Retry frameworks (`tenacity`, `backoff`) — JobSpy retries its own HTTP.
- Heavy CLI libs (`click`, `typer`, `rich`, `loguru`).
- Any cloud DB / object store — the whole point is to keep `jobs.db` in git.

## Files in this directory

- `config.yaml`      — search spec (cities, templates, filters)
- `run.py`           — entry point: `python -m monitor.run`
- `db.py`            — sqlite3 helpers (`setup_db`, `upsert_jobs`, `mark_gone`,
                      `record_run`, `fetch_new_since`, `fetch_active`)
- `notify.py`        — ntfy.sh JSON POST + digest body builder
- `render_md.py`     — JOBS.md generator (tier classification + table render)
- `requirements.txt` — pyyaml + requests (JobSpy is editable-installed)
- `jobs.db`          — committed SQLite state (do NOT gitignore)
- `logs/`            — per-run log files (gitignored, uploaded as artifact on CI failure)

`JOBS.md` lives at the **repo root** (not under monitor/) so GitHub renders it
nicely as the project's primary table view.
