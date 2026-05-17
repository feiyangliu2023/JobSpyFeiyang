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
  linkedin_fetch_description=False, proxies=[...], ...) -> pandas.DataFrame`
- DataFrame columns we care about: `site`, `job_url`, `title`, `company`,
  `location`, `date_posted`, `description`.
- All 8 site values supported by JobSpy: `linkedin`, `indeed`, `glassdoor`,
  `google`, `bayt`, `zip_recruiter`, `naukri`, `bdjobs`.

We currently use **4 of the 8** for EMEA: `indeed`, `linkedin`,
`glassdoor`, `bayt`. zip_recruiter is US/CA only, naukri is India
only, bdjobs is Bangladesh only — none of those help our EMEA focus.
**Google was dropped** (config.yaml, May 2026): the upstream
`speedyapply/JobSpy` Google scraper has been broken since 2025 (see
issue #302), Google Jobs is heavily restricted in the EU/EEA from
server-side IPs (DMA fallout), and 0 rows ever landed in `jobs.db`
from it. (NA is fed entirely by SimplifyJobs, no JobSpy needed there.)

**CI vs local matrix.** GitHub Actions IPs get blanket-403'd by
LinkedIn / Glassdoor / Bayt's edge WAFs before the request reaches
application code, regardless of UA / TLS fingerprint / per-source
header tweaks. So `sites_skip_in_ci: [linkedin, glassdoor, bayt]` on
every template — CI effectively runs Indeed + SimplifyJobs only, and
the user runs locally to fold in LinkedIn / Glassdoor / Bayt.
Setting `JOBSPY_PROXIES` would in principle re-enable them in CI but
we deliberately don't bake a proxy provider into config.

We keep `monitor/` separate from `jobspy/` so upstream pulls don't conflict.
JobSpy rows are tagged `region='emea'` in `run.py` before upsert.

#### Per-site quirks handled in run_search

- **glassdoor**: Glassdoor's `findPopularLocationAjax.htm?term=...`
  endpoint 400's on full "City, Country" strings. `run_search` strips
  to city only ("London, United Kingdom" → "London") just for Glassdoor
  calls. Other sites still get the full string.
- **glassdoor TLD support**: `_glassdoor_supported` checks
  `jobspy.model.Country`'s value tuple — countries without a Glassdoor
  TLD (Sweden, Norway, etc.) get auto-dropped per-city.
- **google**: ignores `search_term`/`location`/`hours_old` separately;
  needs a single `google_search_term` in natural-language form.
  `run_search` synthesizes `"<term> jobs near <location> since last
  <N> days"` from the same fields.
- **bayt**: takes only `search_term`, no location filter. Currently
  enabled for `sde_junior` and `mle_junior` to surface UAE / Israel /
  Saudi roles; dropped from `quant_junior` since quant isn't a Middle
  East category in our experience.

#### Search expansion (cities × templates × sites × terms)

`expand_searches` produces ONE search per `(template, city, site, term)`
tuple. A template has:
- `sites: [list]` — which scrapers to call.
- `search_terms: [list]` — DEFAULT terms for sites without override.
- `site_search_terms: {site: [terms]}` — per-site override. We use this
  to give LinkedIn just one efficient term (LinkedIn does loose title
  matching anyway, and each call burns IP reputation), while letting
  Indeed iterate the whole `search_terms` list (Indeed matches against
  the description, so more terms = more recall).
- `sites_skip_in_ci: [list]` — sites to drop when `GITHUB_ACTIONS` /
  `CI` env var is set. LinkedIn is the only entry by default.

Per-run search counts (current config: 26 cities × 4 templates):
- **LOCAL** mode: ~1112 searches (884 indeed + 104 linkedin + 72 glassdoor + 52 bayt; LinkedIn paced 5s apart). Wall-clock ~20-25 min end to end — LinkedIn pacing alone contributes ~9 min (104 × 5s), Indeed dominates the rest.
- **CI** mode: 884 searches — Indeed only (linkedin/glassdoor/bayt all dropped via `sites_skip_in_ci`; SimplifyJobs externals run on top)

Glassdoor count covers 18 of 26 cities. The other 8 (Sweden, Denmark,
Norway, Finland, Poland, Czech Republic, Portugal, Israel) auto-drop
because their `Country` enum tuple has no Glassdoor TLD entry — see
`_glassdoor_supported`. If JobSpy adds a TLD upstream, those cities
re-enable with no config change here.

#### LinkedIn stability — env-var knobs

LinkedIn aggressively blocks data-center IPs (GitHub Actions ranges
included). The combination that gives us reasonable coverage:

1. **`sites_skip_in_ci: [linkedin, glassdoor, bayt]`** in config —
   dropped automatically when `GITHUB_ACTIONS=true` / `CI=true`. CI
   effectively runs **Indeed only** (plus the SimplifyJobs externals);
   LinkedIn / Glassdoor / Bayt run locally where the user's home IP
   has clean reputation. Glassdoor and Bayt were added to the skip
   list in May 2026 after the post-fix CI run still produced 0 rows
   from both — fixes for both are correct (PR #4) but the WAF block
   is at the network edge, so code-level changes can't help.
2. **`LINKEDIN_PER_SEARCH_DELAY`** env var (default 5s) — sleep AFTER
   each LinkedIn call to spread request density. 104 calls × 5s ≈ 9min
   added to local wall clock.
3. **One efficient term per template** via `site_search_terms.linkedin`.
   Cuts LinkedIn calls from `cities × templates × full_term_list` to
   `cities × templates × 1`.
4. **`JOBSPY_PROXIES`** env var (comma-separated, format
   `user:pass@host:port` or `host:port`) — passed straight to JobSpy's
   `proxies=[...]` param. **Opt-in**, off by default. Set this when
   even local IPs start getting blocked (BrightData / ScraperAPI /
   self-hosted SOCKS / etc).
5. **Block-detection warning** — `run_search` logs a `WARNING` when
   LinkedIn returns 0 rows for any search, with hints on how to recover.
   Easier to grep logs for "LinkedIn returned 0" than to silently lose data.

#### External scrapers — anti-block knobs

JobSpy is one half of the ingestion; the other half is everything
under `monitor/external/` (SimplifyJobs feeds, direct ATS scrapers
for Anthropic / OpenAI / etc). Those used to share nothing — each
file had its own one-line urllib call with a fixed
`jobspy-monitor/1.0` UA and no retry, so a single transient HTTP
failure tanked the source and triggered a SILENT ntfy alert. The
shared helper in `monitor/external/__init__.py` centralizes the
following:

1. **Bounded retry** — `http_get` retries 3 attempts at 2s / 5s / 10s,
   but ONLY on connection errors, 5xx, and 429. Other 4xx (403, 404)
   surface immediately because retrying a hard block just delays the
   inevitable. Deliberately inline — we explicitly avoid `tenacity` /
   `backoff` (see "Do NOT add" below). Wall-clock budget is bounded so
   a hard block fails fast and the health report flags it.
2. **Header pool** — `build_headers()` rotates over 4 recent Chrome
   UAs (Win / macOS / Linux) per call and always sends a real
   `Accept-Language: en-US,en;q=0.9`. Greenhouse and Ashby callers
   additionally pass `Referer` pointing at the ATS board page so the
   request looks like it came from someone browsing the careers site.
   Cheap defenses against trivial UA-filter WAF rules; not pretending
   to be a stealth scraper.
3. **`If-None-Match` / `If-Modified-Since`** — only for `fetch_greenhouse`
   and `fetch_ashby` (the boards that ship validators). Cache files
   live at `monitor/cache/<scraper>.json` (gitignored) and store
   `{etag, last_modified, jobs}`. On a 304 we serve the cached `jobs`
   list; on 2xx we overwrite. Means a partial CDN block / soft 304
   loop keeps producing rows instead of going dark. SimplifyJobs is
   NOT cached this way — its upstream is raw.githubusercontent.com,
   which doesn't honor conditional requests usefully, and a 1s GET
   is fine.
4. **SILENT threshold** — `monitor/health.py:_SILENT_MIN_ATTEMPTS = 3`.
   A source with 0 successes and 0 errors now only classifies as
   SILENT once it has been called at least 3 times; below that it
   stays UNUSED. This prevents single-attempt sources (each `direct:*`
   scraper, naturally low-recall pairings like Bayt + applied
   scientist) from firing false SILENT ntfy alerts. The trade-off is
   that a real block on a single-call source is no longer auto-
   escalated — the per-source row in `health-latest.json` still shows
   `attempts=1, successes=0`, just without the noisy alert.
5. **`JOBSPY_HTTP_DEBUG=1`** env knob — when set, `http_get` logs one
   INFO line per non-2xx response containing the first ~500 bytes of
   the body. Off by default. Useful next time a CDN starts returning a
   block page that looks like a normal failure: grep the log for
   `[http-debug]` to see what the server actually said.

### Secondary feeds: SimplifyJobs (both repos)

`monitor/external/simplify.py` handles two SimplifyJobs-format upstreams,
dispatched by the `type` field in `config.yaml`:

- `simplify_newgrad` → [SimplifyJobs/New-Grad-Positions](https://github.com/SimplifyJobs/New-Grad-Positions) (~10 MB, ~2300 active, **~250 EMEA**)
- `simplify_intern` → [SimplifyJobs/Summer2026-Internships](https://github.com/SimplifyJobs/Summer2026-Internships) (~14 MB, ~2400 active, **~225 EMEA**)

These two files together are the canonical upstream behind every
public "new grad jobs tracker" repo (speedyapply, coderquad-simplify,
etc.) — those repos don't scrape, they render this. We do the same.

A third feed — **`vanshb03_summer2026`** → [vanshb03/Summer2026-Internships](https://github.com/vanshb03/Summer2026-Internships)
(~1.3 MB, ~2100 records, ~150 EMEA after filter) — is wired in as a
SimplifyJobs-schema fork. It uses `type: simplify_intern` with an
explicit `url:` override (no new module needed; the schema is
identical). The dispatch in `run.py` now sets `site_label = name or
kind`, so this source's rows are tagged `site = "vanshb03_summer2026"`
in jobs.db (provenance preserved) while existing rows keep their
`simplify_intern` / `simplify_newgrad` labels (name == type today).

Signature-based dedup in `render_md.py` collapses any posting that
appears in both this feed and SimplifyJobs's canonical intern feed;
SimplifyJobs wins the tie because it ranks higher in
`_SOURCE_PRIORITY`. The value of the fork is the ~10-20% of postings
that vanshb03 catches and the canonical repo doesn't — dedup surfaces
those additively rather than as a duplicate explosion. The
intern/newgrad classifier in `render_md._classify_intern_or_newgrad`
uses substring matching (`"intern" in site`, `"summer202" in site`,
`"newgrad" in site`) so future forks pick the right bucket without
code changes.

**Two other fork repos investigated and skipped (2026-05-14):**

- **`Ouckah/Summer2026-Internships`** — does NOT exist. The Ouckah
  GitHub user has 0 public repos; the repo returns 404 on both `dev`
  and `main`. GitHub repo-search for `Summer2026-Internships` turns
  up SimplifyJobs and a handful of unrelated smaller forks
  (PrepAIJobs, summer2026internships, etc.) — none under Ouckah. Not
  added.
- **`coderquad/New-Grad-2026`** — does NOT exist. The `coderquad`
  user only has `coderQuad/Spring2022-Internships`; no 2026 repo. Not
  added.

If either appears later under a different name, drop a new entry into
`external_sources` with `type: simplify_intern` (interns) or
`simplify_newgrad` (newgrads), an explicit `url:`, and a distinct
`name:` so it gets its own site label. Also add the new name to
`_SOURCE_PRIORITY` in `render_md.py` at the same tier as the existing
SimplifyJobs entries.

### Tertiary feed: speedyapply (4 MD files)

`monitor/external/speedyapply.py` parses the markdown tables in
[speedyapply/2026-SWE-College-Jobs](https://github.com/speedyapply/2026-SWE-College-Jobs).
The repo ships 4 hand-curated MD files; each is wired in as a separate
`type: speedyapply` source with a `file:` discriminator
(`intern_usa` / `newgrad_usa` / `intern_intl` / `newgrad_intl`).

Each file becomes its own `site` label in jobs.db
(`speedyapply_intern_usa`, …) so the substring routing in
`_classify_intern_or_newgrad` ("intern" / "newgrad" in the site)
picks the right bucket without a title regex fallback. Site labels
also match the `speedyapply` token in `_row_is_curated_source`, so
new-grad rows skip the batch-hire title gate (the upstream curation
IS the signal, same as SimplifyJobs / vanshb03).

speedyapply doesn't scrape — it's a curated render of SimplifyJobs +
extras. Ranked BELOW `simplify_*` and `vanshb03_summer2026` in
`_SOURCE_PRIORITY` so the raw upstreams win the signature-dedup tie;
speedyapply rows only survive when they represent postings the
canonical feeds haven't picked up yet. Value-adds:
  - Salary column on new-grad rows (`min_amount` / `max_amount` /
    `currency=USD` / `salary_interval=yearly`) — SimplifyJobs's
    listings.json has no salary field.
  - ~10-20% of postings that the canonical feed hasn't ingested yet
    (same "non-overlapping coverage" rationale as vanshb03).

Parser shape: each MD file has 1-3 GFM tables wrapped in
`<!-- TABLE_*_START -->` / `<!-- TABLE_*_END -->` markers (FAANG /
Quant / Other categories). Cells are HTML-flavoured (`<a><strong>`
for company, `<a><img alt="Apply">` for the apply URL). The Age
column ("11d") converts to `date_posted = today - 11 days`. The MD
ships no description field, so `min_description_chars` must be in
`skip_filters` (same as SimplifyJobs).

USA files default `region=north_america` for unrecognised location
strings ("Milwaukee Wisconsin United States of America" has no comma
suffix the classifier can match); INTL files default `region=other`
so APAC postings get dropped by `allowed_regions=[emea, north_america]`.

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
can `skip_filters` individual gates. We skip `min_description_chars`
(listings.json has no description field — every row would otherwise
drop). **`include_companies` is enforced** for SimplifyJobs as well:
without it the two feeds together surface >2,000 rows on every run, the
vast majority at companies that aren't on the allowlist. The allowlist
is intentionally broad (FAANG, AI labs, scaleups, quant/finance,
consulting) so the curated set still shows up. **Title filters
(`exclude_titles`, `include_title_keywords`) stay ON** across both feeds
— senior/staff/sales-shaped titles are dropped consistently regardless
of source.

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

`upsert_jobs` uses SQLite's `INSERT ... ON CONFLICT(job_url) DO UPDATE
SET ...` (UPSERT, available since SQLite 3.24). One statement per row,
no per-row `SELECT` check. The UPDATE branch keeps the same COALESCE
behavior as the previous if/else version — existing non-null values
win over null incoming values, so an Indeed re-scrape that returns
`description=None` doesn't clobber a SimplifyJobs description from a
prior run. The statement uses `RETURNING first_seen` so the caller can
still distinguish a fresh INSERT from a refresh (first_seen ==
run_started_at iff inserted).

### Three layers of dedup

1. **Same-source / multi-run dedup** — the `job_url` PK. Re-running the
   monitor against unchanged upstream data refreshes `last_seen` instead
   of inserting duplicates. This is sqlite-level and automatic.

2. **Intra-run URL dedup** — `run.py` maintains a per-run `seen_urls`
   set shared across the JobSpy main loop AND `ingest_external_sources`.
   The same `job_url` legitimately surfaces in multiple `(city,
   template, site, term)` batches (e.g. Indeed's "software engineer"
   and "swe" terms can both return the same posting); without this, each
   dup re-fires `upsert_jobs` and wastes ON CONFLICT-update writes.
   Skipped rows are logged as `deduped N intra-run duplicates` and the
   end-of-run summary reports the total.

3. **Cross-source dedup** — the `signature` column, computed by
   `db.compute_signature` from a normalized `(company, title, first
   location city, region)` tuple. Title normalization strips year tokens
   (2026), seniority words (junior, new grad, graduate, etc.), and
   parenthetical suffixes ("(All Genders)", "(f/m/x)") so the same Apple
   London role produces the same signature whether it came from Indeed
   or SimplifyJobs. The `region` component (added 2026-05) disambiguates
   Cambridge UK from Cambridge MA — the first-city normaliser alone
   collapses both to `"cambridge"`, but they're clearly different roles;
   `region` is the suffix-biased location classifier's result from
   `external/locations.py`. The signature is stored on every row but
   the dedup is applied at two places:
   - **Notify-time** (`db.fetch_new_since`) — collapses duplicate roles
     in the ntfy digest so the user gets one entry per role even when
     it surfaced via SimplifyJobs AND `direct:anthropic` in the same
     run. Prefers the higher-priority source per
     `render_md._SOURCE_PRIORITY` / `_DIRECT_PREFIX` (direct:* wins).
   - **Render-time** (`render_md._dedupe_by_signature`) — same logic
     for the rendered tables; rows with the same signature collapse to
     the highest-priority source. Operates within a (region, tier) bucket.

Columns added after the original schema (salary, company_url, is_remote,
region, source_category, signature) live in `_MIGRATIONS`; `setup_db`
runs `ALTER TABLE ADD COLUMN` statements wrapped in try/except so older
DBs upgrade in place. On every startup `setup_db` runs two cheap
backfills: legacy region rows → 'emea', and an unconditional signature
recompute for every row (the format evolved when `region` was added; we
recompute rather than maintain a schema_version table just for this).

### Retention (--retention-days, default 180)

`db.prune_old(conn, retention_days)` runs once per run, called from
`run.py` AFTER `mark_gone` and BEFORE the render step. Two deletes plus
a VACUUM so the on-disk file actually shrinks (without VACUUM, SQLite
keeps freed pages around for reuse and the file size never drops — that's
why ~10 consecutive `chore(monitor): refresh` commits were dominating
.git size growth):

  - `jobs`: drop rows where `status='gone' AND last_seen < (now -
    retention_days)`. Active rows are NEVER pruned — they still appear
    in JOBS.md and the renderer needs them.
  - `runs`: drop rows where `started_at < (now - runs_retention_days)`.
    The runs table is purely diagnostic and accumulates ~30-60 rows per
    scrape (one per city-site combo), so it would dominate jobs.db
    within a few months at the older `retention_days // 2` window.
    `run.py` passes `--runs-retention-days` (default 30) explicitly;
    callers that omit it fall back to `retention_days // 2` for
    backwards compat with the existing tests.

VACUUM only runs when something was deleted (no point shuffling pages
on a no-op run). Logs counts pruned per table at INFO. Pruning failures
are swallowed — JOBS.md can still render from the un-pruned set if
prune blows up, so we don't fail the whole run.

The `--retention-days` argparse flag controls the jobs window (default
180 days — a full half-year, matches how often roles come back under
the same URL, rarely beyond ~3 months). The `--runs-retention-days`
flag controls the diagnostic runs window separately (default 30 days).
Lower either locally to inspect prune behavior on a fresh DB.

### Render caps (MD bloat guard)

`render_md.RENDER_MAX_ROWS` (default 500) and `RENDER_MAX_AGE_DAYS`
(default 180) bound the size of every rendered MD section so JOBS.md /
slice files / emea-graduate.md stay browsable as the broader pool
grows. Applied via `_apply_render_caps(rows, ...)` in three call sites:
`render_slice`, `render_md` (per region), `render_region_graduate`.
Caps affect what gets WRITTEN, not what gets STORED — the full row set
stays in jobs.db until the normal `prune_old` lifecycle catches it.

When a cap drops rows, `_render_cap_note(...)` appends a one-line
italic footer beneath the table explaining what was hidden. Section
header counts continue to advertise the TRUE active count (pre-cap)
so users can tell when a cap is in play. Per-slice overrides via
`max_rows` / `max_age_days` in slices.yaml; 0 / None disables that side
of the cap.

## JOBS.md (rendered table)

`render_md.render_md(active_rows, path)` writes `JOBS.md` at the repo root
on every run. Layout is one comprehensive table per region:

```
EMEA (primary)               → 1 table, sorted newest-first
North America (via SimplifyJobs) → 1 table, sorted newest-first
```

EMEA always renders FIRST regardless of row counts — that's the user's
primary feed. Within each region the rows are deduped by signature and
sorted by date (newest first); the Age column carries freshness without
needing tier or time-bucket sub-sections.

Each table is wrapped in `<!-- TABLE_<REGION>_START -->` /
`<!-- TABLE_<REGION>_END -->` HTML comment markers (mirroring the
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

## emea-graduate.md / na-graduate.md (broader view)

Two markdown files at the repo root, generated by the same run, sit
beside JOBS.md as **wider-net browses** of full-time new-grad roles —
one per region. Both are produced by the shared
`render_region_graduate(rows, region, path)`; `render_emea_graduate`
and `render_na_graduate` are thin region-specific wrappers.

Internship rows are dropped from this view entirely — they're covered
by the per-region internship slice files (`emea-internships.md` /
`na-internships.md`) generated from `slices.yaml`.

Differences from `JOBS.md`:

- **No company allowlist gate** — runs the same title (`exclude_titles`,
  `include_title_keywords`) and `min_description_chars` filters but skips
  `include_companies`. So a London Klarna or Vinted role that's not on
  the curated allowlist still surfaces here.
- **One file per region**, scoped via `render_region_graduate`'s
  `region` argument (`emea` / `north_america`). Other regions are
  dropped client-side at render time.
- **Layout: one full-time new-grad table.** Rows are deduped + sorted
  newest-first; no further bucketing. The intern/newgrad classifier
  still runs upstream to filter intern rows out of this view.
- **Stateless** — these rows do NOT go into `jobs.db`. Files are
  recomputed from in-memory data every run (JobSpy's pre-allowlist EMEA
  rows + a no-allowlist pass over the SimplifyJobs feeds). This keeps
  jobs.db curated for ntfy alerting and avoids inflating it by ~hundreds
  of off-allowlist rows.
- **No ntfy.** Net-new entries here don't trigger the digest — that
  channel stays focused on JOBS.md's curated set.

### Entry-level title filter (`_title_passes_entry_level_filter`)

Targets speedyapply's [2026-SWE-College-Jobs](https://github.com/speedyapply/2026-SWE-College-Jobs)
NEW_GRAD / INTERN shape — i.e. batch-hire intake at big companies, not
single-headcount specific roles. Three gates applied in order:

1. **Seniority drop** — drops `senior` / `staff` / `principal` /
   `lead engineer` / `manager` / `director` / `vp` / academic titles
   (`lecturer`, `professor`, `postdoc`). Caught here rather than
   relying on `exclude_titles` because that list also strips tokens
   we WANT in entry-level (e.g. `internship` is excluded for senior
   search-term recall but obviously must stay for intern roles).
2. **Tech-shape required** — title must contain a software / ML / data
   token (`software engineer`, `ml engineer`, `applied scientist`,
   `data analyst`, `swe`, `sde`, `sre`, `machine learning`, `algorithm`,
   `member of technical staff`, …). Drops "Vehicle Testing Engineer
   Internship", "Process Engineering Intern", "Graduate Project
   Engineer", etc. Bare `ml` / `ai` standalone are also accepted via
   word-boundary regex so titles like "Anthropic Fellows Program — ML
   Systems" pass.
3. **Batch-hire marker required for new-grad** — only enforced for rows
   from raw aggregators (JobSpy: Indeed / LinkedIn / Glassdoor; or
   `direct:*` ATS scrapers, which pull every open role at the company
   without curation). Required markers: `new grad`, `graduate`,
   `university hire`, `campus`, `early career`, `entry level`,
   `class of 20XX`, `Software Engineer I` / `SDE 1`, `junior X`,
   `associate X`, `apprentice`, `trainee`, `fellowship`, `residency`.
   **Curated upstream feeds bypass this gate** (`simplify_newgrad`,
   `simplify_intern`, `vanshb03_summer2026`, …) — repo membership IS
   the new-grad signal, so a bare-titled SimplifyJobs row like
   "Software Engineer" still passes. This is what guarantees we match
   speedyapply's coverage on the curated side.

### Intern vs new-grad classification (`_classify_intern_or_newgrad`)

- SimplifyJobs rows: site label tells us (`simplify_intern` →
  intern, `simplify_newgrad` → newgrad) — the upstream repo split is
  the source of truth. The site check uses substring matching
  (`"intern" in site`) so forks like `vanshb03_summer2026` bucket
  correctly.
- JobSpy / direct rows: regex match on title (`\bintern(s|ship|...)\b`,
  `\bplacement\b`, `\byear in industry\b`, `\bpraktikum\b`, `\bstage\b`
  with hyphen-aware boundaries, `\bstagiaire?\b`, `\bbecari[oa]\b`,
  `\btirocinante\b`, `\btrainee\b`). Default → newgrad.
  **Word-boundary regex matters**: bare substring `"intern" in title`
  wrongly matches `Internal` / `International` / `Internet`, routing
  Senior / SRE / Lead roles into the intern bucket. The regex pattern
  `\bintern(?:s|ship|ships|ing)?\b` allows only the legitimate
  suffixes (or no suffix), so `Internal` no longer false-matches.

Wired through `run.py` via:
- A `broader_rows` list collected in the JobSpy main loop (running
  `apply_filters` a second time with `skip={"include_companies"}`) and
  passed as `broader_sink` into `ingest_external_sources` (which appends
  a parallel no-allowlist EMEA + NA cut from each SimplifyJobs feed —
  region tagging is preserved per-row so EMEA / NA slice files narrow
  correctly downstream). Before rendering, `_enrich_broader_rows_from_db`
  copies `first_seen` + liveness columns from jobs.db onto broader rows
  that match an allowlisted row (off-allowlist rows get
  `first_seen=this_run` and NULL liveness → rendered as "(?)").
- `render_emea_graduate()` / `render_na_graduate()` filter this pool
  to one region and write the files at end of run, after `render_md()`
  produces JOBS.md.
- The same `broader_rows` pool feeds `render_slices()` — slice files
  (emea-junior-sde.md, na-junior-sde.md, …) are comprehensive views,
  NOT allowlist-gated. JOBS.md remains the curated allowlist-gated table.
- **Slice surface (May 2026)** — beyond junior SDE / MLE / intern /
  quant, slices.yaml now defines per-region data-analyst, algorithm
  (算法岗), solutions/customer/devops, and senior SDE views. The new
  role-type slices are NOT junior-gated (they accept all seniority);
  senior-SDE-only enumerates seniority-prefixed phrasings explicitly.
  Existing junior slices keep their local `title_keywords_none` block,
  so the senior tokens lifted from `exclude_titles` don't leak in.
- Paths are overridable via `--md-emea-graduate` /
  `--md-na-graduate`; defaults are `<repo_root>/emea-graduate.md`
  and `<repo_root>/na-graduate.md` to mirror JOBS.md's location.

## Known issues

- **LinkedIn / Glassdoor / Bayt block GitHub Actions IPs aggressively.**
  Mitigated by `sites_skip_in_ci: [linkedin, glassdoor, bayt]` on every
  template — CI runs Indeed + SimplifyJobs only; LinkedIn / Glassdoor /
  Bayt run locally with pacing. If even local fails, set
  `JOBSPY_PROXIES`. Do NOT make proxies a hardcoded default.
- **Glassdoor location format quirk** — fixed. `run_search` strips
  `"City, Country"` to `"City"` only when calling Glassdoor; other
  sites still get the full string. The `_glassdoor_supported` check
  also drops cities whose country has no Glassdoor TLD (currently
  Sweden, Denmark, Norway, Finland, Poland, Czech Republic, Portugal,
  Israel) — those get auto-skipped per-city in `expand_searches`.
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

- ~~Proxies / rotating IPs~~ — superseded. Proxy support is now opt-in
  via `JOBSPY_PROXIES` env var (see "LinkedIn stability" section above).
  The default is still no proxies; only enable when LinkedIn / Indeed
  start failing even on local. Don't bake a specific proxy provider
  into config — keep the env-var indirection.
- `asyncio` / `aiohttp` (JobSpy already does its own thread pool internally).
- An ORM (SQLAlchemy, Peewee — raw `sqlite3` is plenty for this scale).
- LLM-based filtering (latency, cost, non-determinism for a cron job).
- Retry frameworks (`tenacity`, `backoff`) — JobSpy retries its own HTTP.
- Heavy CLI libs (`click`, `typer`, `rich`, `loguru`).
- Any cloud DB / object store — the whole point is to keep `jobs.db` in git.

## Health reporting

Per-source success/failure accounting is handled by `monitor/health.py`.
The flow:

1. `run.main` instantiates a `HealthTracker` once per run.
2. The JobSpy main loop and `ingest_external_sources` call `record_*`
   on it as they go. JobSpy sources use the bare site name as the key
   (`linkedin`, `indeed`, `glassdoor`, `google`, `bayt`); external
   sources use the `external:<name>` form so they don't collide.
3. At end of run, three things happen:
   - **Always**: a per-source table is logged at INFO level (visible
     in `logs/run.log` and stdout). One status glyph per source:
     `OK / DEG / SLT / BRK / —`.
   - **Always**: a JSON dump goes to `logs/health-latest.json`. This
     file overwrites every run; grep-friendly enough to inspect via
     `jq '.sources | with_entries(select(.value.status != "OK"))'`.
   - **When any source is non-OK**: an ntfy push is sent via
     `notify.send_health_alert`. Tagged `warning,construction` and
     priority 4 (vs digest's 3) so it's distinguishable from the daily
     "new jobs" digest.

Status classification thresholds:
- `OK`        — at least one call returned ≥1 row, success rate ≥ 30%, no errors > attempts.
- `DEGRADED`  — some calls succeeded but rate < 30% (flaky source / partial rate-limit).
- `SILENT`    — every call returned 0 rows AND no exception thrown
                (statistically improbable across 50+ searches → almost
                always an IP block).
- `BROKEN`    — every call threw an exception (URL down, schema change).
- `UNUSED`    — never invoked (config flag, `sites_skip_in_ci`, etc.).

`run.main` returns exit code 2 when `overall_status()` is `BROKEN` or
`SILENT` so a cron / CI runner fails loudly. `DEGRADED` is non-fatal —
expect Google Jobs in particular to drift between OK and DEGRADED
depending on the day's search-term recall.

Toggles:
- `NTFY_HEALTH_ALERTS=0` env var — disables the ntfy push (still logs
  + writes JSON). Useful when iterating locally and you don't want to
  spam your phone.
- `NTFY_TOPIC` unset — no push (same as digest).

## Files in this directory

- `config.yaml`      — search spec (cities, templates, filters, external_sources)
- `run.py`           — entry point: `python -m monitor.run`
- `db.py`            — sqlite3 helpers (`setup_db`, `upsert_jobs`, `mark_gone`,
                      `record_run`, `fetch_new_since`, `fetch_active`, `prune_old`)
- `health.py`        — per-source HealthTracker + SourceStats + classification
- `notify.py`        — ntfy.sh JSON POST + digest body builder + health alert
- `render_md.py`     — JOBS.md / slice / INDEX generator (per-region table render)
- `external/`        — non-JobSpy ingestion modules
   `external/simplify.py`    — SimplifyJobs listings.json fetcher + schema mapper (handles both new-grad and intern repos)
   `external/speedyapply.py` — speedyapply/2026-SWE-College-Jobs MD parser (4 files: USA / INTL × intern / newgrad)
   `external/locations.py`   — location string → (country, region) classifier; suffix-biased to disambiguate Cambridge UK vs MA, Birmingham UK vs AL, etc.
- `tests/`           — pytest suite for `db.py` (signature edge cases, upsert,
                      prune). Run with `python -m pytest monitor/tests/`.
- `requirements.txt` — pyyaml + requests + pytest (JobSpy is editable-installed)
- `jobs.db`          — committed SQLite state (do NOT gitignore)
- `logs/`            — per-run log files (gitignored, uploaded as artifact on CI failure)

`JOBS.md` lives at the **repo root** (not under monitor/) so GitHub renders it
nicely as the project's primary table view.

`emea-graduate.md` / `na-graduate.md` live there too, for the same
reason. See the dedicated section above for what's different about
them.
