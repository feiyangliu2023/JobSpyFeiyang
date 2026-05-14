# `monitor/external/direct/` — per-company careers-page scrapers

SimplifyJobs is hand-curated, so a fresh posting can sit unmerged for
hours-to-days. JobSpy bottlenecks on aggregator rate limits (LinkedIn /
Indeed) and only catches roles that the aggregator itself indexed. For
the user's tier-1 target companies neither lag is acceptable — this
directory hits each company's ATS feed directly so a new role lands in
`JOBS.md` within one cron tick.

## What lives here

| File | Purpose |
|------|---------|
| `__init__.py` | Shared helpers — `fetch_greenhouse`, `fetch_ashby`, `make_row`. |
| `anthropic.py` | Greenhouse, board=`anthropic`. |
| `openai.py`   | Ashby, board=`openai`. |
| `mistral.py`   | Ashby, board=`mistral`. |
| `cohere.py`    | Ashby, board=`cohere`. |
| `scaleai.py`   | Ashby, board=`scaleai`. |
| `stripe.py`     | Greenhouse, board=`stripe`. |
| `databricks.py` | Greenhouse, board=`databricks`. |
| `deepmind.py` | Stub. Google Careers is JS-rendered + bot-gated; see file for rationale. |

Each company file is intentionally tiny (~10 lines) — all the parsing
work is in `__init__.py` so adding the next company is mechanical.

## The pattern

A direct scraper exports **one function** with **one signature**:

```python
def fetch_listings() -> list[dict]: ...
```

The returned rows must match the same shape `monitor.external.simplify.to_rows`
produces (the keys consumed by `monitor.db.upsert_jobs`):

```
job_url, site, title, company, company_url, location, is_remote,
date_posted (YYYY-MM-DD or None), description, region, source_category,
min_amount, max_amount, currency, salary_interval
```

`make_row(...)` in `__init__.py` builds this dict from raw fields — use
it. It calls `classify_locations` so `region` is set correctly per row
(EMEA / north_america / other), which the YAML's `allowed_regions` then
filters on.

Tag rows with `site="direct:<company>"`. The colon prefix is what tells
`render_md._source_rank` to promote these rows above `simplify_*` during
cross-source dedup.

## Wiring a new company

1. Identify the ATS. Visit the company's careers page and look at
   network requests in DevTools, or check the apply URL:
   - `boards.greenhouse.io/<token>` → Greenhouse
   - `jobs.ashbyhq.com/<board>` → Ashby
   - `jobs.lever.co/<co>` → Lever
   - `<co>.recruitee.com` → Recruitee
   - `careers.smartrecruiters.com/<co>` → SmartRecruiters
2. Find the board token (the URL slug).
3. Write `<company>.py`:

   ```python
   """Mistral careers (Ashby-backed)."""
   from monitor.external.direct import ashby_to_rows, fetch_ashby

   SITE_LABEL = "direct:mistral"
   BOARD_NAME = "mistral"
   COMPANY = "Mistral AI"
   COMPANY_URL = "https://mistral.ai/"

   def fetch_listings() -> list[dict]:
       jobs = fetch_ashby(BOARD_NAME)
       return ashby_to_rows(
           jobs, site_label=SITE_LABEL,
           company=COMPANY, company_url=COMPANY_URL,
       )
   ```

4. Add a config entry in `monitor/config.yaml` under `external_sources`:

   ```yaml
   - name: direct_mistral
     type: direct
     module: mistral
     allowed_regions: [emea, north_america]
     skip_filters:
       - min_description_chars
   ```

5. Make sure the company is on `filters.include_companies` (it is for
   all the user's tier-1 targets — Mistral, Cohere, Stripe, Databricks,
   Scale are already there). Otherwise the global allowlist will drop
   every row.

That's it — no changes to `run.py` or `render_md.py` are needed.

## ATS cheat sheet — next tier (Workday-gated, not yet added)

The 5 companies the previous cheat sheet listed (Mistral, Cohere,
Stripe, Databricks, Scale AI) have all been added as of May 2026 —
they're listed in the "What lives here" table above.

The remaining tier-1 NA companies are all behind Workday-style ATSes
that need a headless browser:

| Company   | ATS          | Why it's not here yet |
|-----------|--------------|-----------------------|
| Google    | Custom + JS  | See `deepmind.py` stub for rationale. |
| Meta      | Workday      | Same problem class. |
| Amazon    | Custom + JS  | amazon.jobs is reachable via SimplifyJobs new-grad only. |
| Microsoft | Custom + JS  | Same. |
| Apple     | Custom + JS  | Same. |

Adding any of the above means standing up a Playwright/Selenium pipeline
plus the maintenance cost of session/token rotation; out of scope until
the demand justifies it. SimplifyJobs covers the new-grad slice; the
senior slice for those companies stays a gap.

**Verify the token before committing.** ATSes occasionally move
companies between hostnames. A 30-second smoke test is enough:

```bash
py -3 -c "from monitor.external.direct import fetch_greenhouse; \
  print(len(fetch_greenhouse('stripe')))"
```

## When NOT to add a direct scraper

- **The company uses Workday, SuccessFactors, Taleo, or its own
  in-house ATS.** These hide listings behind session tokens, captchas,
  or JS rendering. Cost of building + maintaining a scraper outweighs
  the savings over SimplifyJobs. Document the choice in a stub module
  the same way `deepmind.py` does.
- **The company has <5 EMEA roles per year and SimplifyJobs already
  covers them reliably.** SimplifyJobs's lag is fine for sparse
  posters; direct scraping is for surface companies where a 24h delay
  matters.
- **The company isn't on the `include_companies` allowlist.** The
  global filter will drop every row regardless — wire the allowlist
  first.

## Failure modes to know

- **Board token changed.** Greenhouse and Ashby both 404 silently when
  a board is renamed; the scraper logs a 404 and returns `[]`, the
  end-of-run health table flags the source `BROKEN`. Update the
  `BOARD_TOKEN` constant in the company module.
- **API rate-limit / Cloudflare challenge.** Neither Greenhouse nor
  Ashby rate-limits their public boards in practice (one request per
  scrape × twice-daily cron is well under any threshold). If it ever
  happens, the source goes `BROKEN` in the health report and we fall
  back to SimplifyJobs + JobSpy automatically.
- **Schema drift.** The Greenhouse / Ashby JSON shapes have been stable
  for years, but if a key disappears `make_row` will produce a row
  with the missing field as `None` rather than crashing — the row
  still makes it to the DB minus that field.
