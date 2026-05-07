# monitor/ — context for future sessions

## Purpose

Twice-daily scheduled scrape of EMEA junior-level SDE and entry-level AI roles
(MLE, Applied Scientist) at large tech companies — that's the user's primary
use case ("我自己要用"). A secondary North America feed pulls daily from
SimplifyJobs/New-Grad-Positions for awareness; both feeds share one
`jobs.db` and one `JOBS.md`.

Net-new postings (deduped against `jobs.db`) are pushed as a single digest
to ntfy.sh per run. The user's phone subscribes to an ntfy topic; that's
the only delivery channel.

## Data sources

Two feeds, one DB. They're tagged on the `region` column.

### EMEA (primary): JobSpy

This directory sits alongside the upstream JobSpy package (the fork's
`jobspy/` folder at the repo root). We `pip install -e ..` so JobSpy is
importable as `jobspy`. The relevant API:

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
JobSpy rows are tagged `region='emea'` in `run.py` before upsert.

### Secondary feeds: SimplifyJobs (both repos)

`monitor/external/simplify.py` handles two SimplifyJobs-format upstreams,
dispatched by the `type` field in `config.yaml`:

- `simplify_newgrad` → [SimplifyJobs/New-Grad-Positions](https://github.com/SimplifyJobs/New-Grad-Positions) (~10 MB, ~2300 active, **~250 EMEA**)
- `simplify_intern` → [SimplifyJobs/Summer2026-Internships](https://github.com/SimplifyJobs/Summer2026-Internships) (~14 MB, ~2400 active, **~225 EMEA**)

These two files together are the canonical upstream behind every
public "new grad jobs tracker" repo (speedyapply, coderquad-simplify,
etc.) — those repos don't scrape, they render this. We do the same.

SimplifyJobs schema (verified by inspecting raw listings.json):
`id, source, category, company_name, title, active, date_updated,
date_posted (unix epoch), url, locations[], company_url, is_visible,
sponsorship, degrees[]`.

We drop rows where `active=false` or `is_visible=false` (SimplifyJobs's
own "this is gone / internal" flags). For each remaining row, we
auto-classify `region` by parsing its `locations[]` strings via
`monitor.external.locations.classify_locations` — so a London Cohere
posting from a "US-centric" repo lands in our EMEA section, while
"Cambridge, MA" stays in NA (the location classifier is suffix-biased
and disambiguates Cambridge UK / Cambridge MA correctly).

Sources can declare `allowed_regions: [emea, north_america]` to drop
APAC rows entirely — that's our default config.

The same `apply_filters` pipeline runs on the mapped rows, but a source
can `skip_filters` individual gates. We skip `include_companies`
(SimplifyJobs is broader than our EMEA-tuned allowlist) and
`min_description_chars` (listings.json has no description field).
**Title filters (`exclude_titles`, `include_title_keywords`) stay ON**
across both feeds — senior/staff/sales-shaped titles are dropped
consistently regardless of source. That's the user's "保留关键词筛选
灵活性" requirement: keyword filtering is universal, allowlist is per-source.

Adding more sources later: drop a new entry into `external_sources`
with a known `type`. Schema-compatible repos (vanshb03/, etc.) just
need a different URL. Schema-incompatible sources need a new module
under `monitor/external/`.

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
  job_url        PRIMARY KEY,        -- per-source dedup key (same posting from same source over multiple runs collapses here)
  site, title, company, company_url, location, is_remote,
  date_posted, description, search_name,
  min_amount, max_amount, currency, salary_interval,  -- nullable; populated when the source returns salary data
  region          TEXT,              -- 'emea' | 'north_america' | 'other' — auto-classified for SimplifyJobs, hardcoded 'emea' for JobSpy
  source_category TEXT,              -- SimplifyJobs's category string ('Software', 'AI/ML/Data', 'Quant', ...) — null for JobSpy rows
  signature       TEXT,              -- cross-source dedup key (see below)
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
All feeds share the same `mark_gone` pass — SimplifyJobs rows whose
`active=false` upstream simply don't get re-ingested, so they age out via
the same mechanism.

### Two layers of dedup

1. **Same-source / multi-run dedup** — the `job_url` PK. Re-running the
   monitor against unchanged upstream data refreshes `last_seen` instead
   of inserting duplicates. This is sqlite-level and automatic.

2. **Cross-source dedup** — the `signature` column, computed by
   `db.compute_signature` from a normalized `(company, title, first
   location city)` tuple. Title normalization strips year tokens (2026),
   seniority words (junior, new grad, graduate, etc.), and parenthetical
   suffixes ("(All Genders)", "(f/m/x)") so the same Apple London role
   produces the same signature whether it came from Indeed or SimplifyJobs.
   The signature is stored on every row but the dedup itself happens at
   render time (`render_md._dedupe_by_signature`) so we keep multi-source
   provenance in the DB. When duplicates exist the renderer prefers the
   higher-priority source — SimplifyJobs first (direct apply URLs),
   then LinkedIn, then Indeed (Indeed is usually a referral chain).

Columns added after the original schema (salary, company_url, is_remote,
region, source_category, signature) live in `_MIGRATIONS`; `setup_db`
runs `ALTER TABLE ADD COLUMN` statements wrapped in try/except so older
DBs upgrade in place. Two backfills run on every startup (cheap, only
touch `WHERE x IS NULL` rows): legacy region rows → 'emea', legacy
signature rows → computed from existing fields.

## JOBS.md (rendered table)

`render_md.render_md(active_rows, path)` writes `JOBS.md` at the repo root
on every run. Layout is a single file (the user explicitly chose not to
split per-region) with a Region × Tier grid:

```
EMEA (primary)
  ├─ FAANG+ & AI Labs
  ├─ Quant & Finance
  └─ Other
North America (via SimplifyJobs)
  ├─ FAANG+ & AI Labs
  ├─ Quant & Finance
  └─ Other
```

EMEA always renders FIRST regardless of row counts — that's the user's
primary feed. Tier classification is a hardcoded substring match in
`render_md.py`; deliberately separate from the strict word-boundary
`_match_company` allowlist gate, because at render time we just need a
quick bucket label, not a precision filter.

Each table is wrapped in `<!-- TABLE_<REGION>_<TIER>_START -->` /
`<!-- TABLE_<REGION>_<TIER>_END -->` HTML comment markers (mirroring the
SimplifyJobs / speedyapply convention) so future tooling can do
partial-replace on a hand-curated outer file. Today we still write the
whole file; the markers are forward-compat.

The Salary column is hidden globally whenever no active row carries
salary data — typical for EMEA Indeed scrapes which almost never return
salary. The column reappears automatically once any row has data.

Age is computed from `date_posted` first, falling back to `first_seen`
(scraper's first sighting). This matters because some Indeed listings
come back with a 6-month-old `date_posted` while `first_seen` is today,
which would otherwise show as "0d" — misleading.

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

- `config.yaml`      — search spec (cities, templates, filters, external_sources)
- `run.py`           — entry point: `python -m monitor.run`
- `db.py`            — sqlite3 helpers (`setup_db`, `upsert_jobs`, `mark_gone`,
                      `record_run`, `fetch_new_since`, `fetch_active`)
- `notify.py`        — ntfy.sh JSON POST + digest body builder
- `render_md.py`     — JOBS.md generator (Region × Tier grid + table render)
- `external/`        — non-JobSpy ingestion modules
   `external/simplify.py` — SimplifyJobs listings.json fetcher + schema mapper (handles both new-grad and intern repos)
   `external/locations.py` — location string → (country, region) classifier; suffix-biased to disambiguate Cambridge UK vs MA, Birmingham UK vs AL, etc.
- `requirements.txt` — pyyaml + requests (JobSpy is editable-installed)
- `jobs.db`          — committed SQLite state (do NOT gitignore)
- `logs/`            — per-run log files (gitignored, uploaded as artifact on CI failure)

`JOBS.md` lives at the **repo root** (not under monitor/) so GitHub renders it
nicely as the project's primary table view.
