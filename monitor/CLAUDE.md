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
  job_url      PRIMARY KEY,  -- the dedup key
  site, title, company, location, date_posted, description, search_name,
  first_seen   TEXT NOT NULL,  -- ISO8601 UTC, set on insert
  last_seen    TEXT NOT NULL,  -- ISO8601 UTC, refreshed on each upsert
  status       TEXT NOT NULL   -- 'active' or 'gone'
)

runs(
  run_id       INTEGER PK,
  started_at, search_name, rows_scraped, rows_new
)
```

"New" = rows whose `first_seen` equals this run's timestamp. After the upsert
loop, any active row whose `last_seen < this_run` is flipped to `'gone'`.

## Known issues

- **LinkedIn blocks GitHub Actions IPs aggressively.** Expect sparse or empty
  LinkedIn results from CI. The user accepts this and runs locally as a
  fallback (`python -m monitor.run`). Do NOT add proxy logic to "fix" it.
- **Glassdoor is unavailable for several countries** (Sweden, Norway, etc.).
  `expand_searches` drops `glassdoor` from the site list when the Country
  enum has no glassdoor TLD. The fallback set is hardcoded only for
  resilience if the import fails.
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

## Do NOT add without asking

- Proxies / rotating IPs (won't help reliably and adds cost).
- `asyncio` / `aiohttp` (JobSpy already does its own thread pool internally).
- An ORM (SQLAlchemy, Peewee — raw `sqlite3` is plenty for this scale).
- LLM-based filtering (latency, cost, non-determinism for a cron job).
- Retry frameworks (`tenacity`, `backoff`) — JobSpy retries its own HTTP.
- Heavy CLI libs (`click`, `typer`, `rich`, `loguru`).
- Any cloud DB / object store — the whole point is to keep `jobs.db` in git.

## Files in this directory

- `config.yaml`     — search spec (cities, templates, filters)
- `run.py`          — entry point: `python -m monitor.run`
- `db.py`           — sqlite3 helpers (`setup_db`, `upsert_jobs`, `mark_gone`,
                     `record_run`, `fetch_new_since`)
- `notify.py`       — ntfy.sh JSON POST + digest body builder
- `requirements.txt` — pyyaml + requests (JobSpy is editable-installed)
- `jobs.db`         — committed SQLite state (do NOT gitignore)
- `logs/`           — per-run log files (gitignored, uploaded as artifact on CI failure)
