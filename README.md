# Junior Tech Jobs Tracker

A curated, auto-refreshed list of **junior / new-grad / internship tech roles**
in EMEA and North America — built primarily so I can use it for my own job
search, and shared in case it's useful to anyone else hunting for the same
slice of the market.

Forked from [cullenwatson/JobSpy](https://github.com/cullenwatson/JobSpy) and
extended with a scheduled scraper, a SQLite state store, multi-source
ingestion (JobSpy + SimplifyJobs feeds + direct ATS scrapers for AI labs), and
a renderer that produces the markdown tables below. The upstream JobSpy
package is still vendored at [`jobspy/`](jobspy/) and used as a library.

## Browse the jobs

Start at **[INDEX.md](INDEX.md)** for the live table of contents with current
active counts and last-liveness timestamps next to every slice. The slices
below are overwritten on every run (roughly every 8 hours).

### Curated, allowlist-gated (FAANG+, AI labs, quant, scaleups)

- **[JOBS.md](JOBS.md)** — one big region-grouped table; the canonical view.

### Slices by track (no company allowlist, title-filtered only)

Pick the row that matches the kind of role you're hunting for, then click the
region you care about.

| Track | EMEA | North America |
|---|---|---|
| Junior SDE | [emea-junior-sde.md](emea-junior-sde.md) | [na-junior-sde.md](na-junior-sde.md) |
| Junior MLE / Applied Scientist | [emea-junior-mle.md](emea-junior-mle.md) | [na-junior-mle.md](na-junior-mle.md) |
| Internships | [emea-internships.md](emea-internships.md) | [na-internships.md](na-internships.md) |
| Senior SDE | [emea-senior-sde.md](emea-senior-sde.md) | [na-senior-sde.md](na-senior-sde.md) |
| Data Analyst | [emea-data-analyst.md](emea-data-analyst.md) | [na-data-analyst.md](na-data-analyst.md) |
| Algorithm (算法岗) | [emea-algorithm.md](emea-algorithm.md) | [na-algorithm.md](na-algorithm.md) |
| Quant & Finance | [emea-quant.md](emea-quant.md) | [na-quant.md](na-quant.md) |
| Solutions / Customer / DevOps | [emea-solutions-devops.md](emea-solutions-devops.md) | [na-solutions-devops.md](na-solutions-devops.md) |

### Widest browse (no allowlist, no track filter)

- **[emea-entry-level.md](emea-entry-level.md)** — every EMEA intern +
  new-grad role that passes the title filter, regardless of company.
- **[na-entry-level.md](na-entry-level.md)** — same, for North America.

### Cross-region

- **[remote-jobs.md](remote-jobs.md)** — remote-eligible postings across
  EMEA and NA, including smaller startups outside the curated allowlist.

Each table includes a "New in last 24h" and "New in last 7d" section at the
top, an apply link, and a liveness indicator — `(?)` means the URL hasn't yet
been verified to still be live.

> Slice definitions (which titles match what) live in
> [`monitor/slices.yaml`](monitor/slices.yaml). If a slice doesn't surface the
> roles you expect, that file is where the title-keyword gates are tuned.

## Sources

- **JobSpy scrapers** (Indeed, LinkedIn, Glassdoor, Bayt) across 26 EMEA
  cities. LinkedIn / Glassdoor / Bayt are run locally only; GitHub Actions
  IPs get edge-blocked by their WAFs.
- **SimplifyJobs** new-grad and Summer 2026 internship feeds — the canonical
  upstream behind most public "new grad jobs" trackers.
- **vanshb03/Summer2026-Internships** — SimplifyJobs-schema fork that catches
  ~10-20% of postings the canonical feed misses.
- **Direct ATS scrapers** for tier-1 AI labs (Anthropic, OpenAI, …) via
  Greenhouse / Ashby boards, with conditional-GET caching so transient CDN
  blocks don't drop coverage.

Cross-source duplicates are collapsed by a normalised
`(company, title, location, region)` signature so the same Apple London role
surfaced via Indeed + SimplifyJobs appears once.

## How it runs

GitHub Actions cron at **07:00 UTC** and **15:00 UTC** runs
`python -m monitor.run`, which:

1. Expands `monitor/config.yaml` into ~900 searches in CI (~1100 locally) and
   calls each source.
2. Upserts results into `monitor/jobs.db` (committed back to the repo for
   state across runs; pruned to 180 days).
3. Marks any previously-active row not seen this run as `gone`.
4. Re-renders `JOBS.md`, `INDEX.md`, the per-region entry-level files, and
   every slice listed in [`monitor/slices.yaml`](monitor/slices.yaml).
5. Sends a single per-run digest of net-new postings to my phone via
   [ntfy.sh](https://ntfy.sh) (no notification when nothing new — no spam).
6. Emits a per-source health alert if any feed silently stops returning
   rows.

The pipeline, schema, dedup strategy, and per-source quirks are documented in
[`monitor/CLAUDE.md`](monitor/CLAUDE.md) — easily the longest file in the
repo.

## Fork it for your own search

The pipeline isn't EMEA / junior-specific; everything that filters is in
[`monitor/config.yaml`](monitor/config.yaml):

- `cities` — swap in your own hubs (each city becomes a JobSpy search bucket).
- `role_templates` — change `search_terms` and add per-site overrides.
- `filters.include_companies` — the allowlist that feeds `JOBS.md`. Slice
  files and the two `*-entry-level.md` files ignore this gate, so they keep
  working even if you delete the allowlist entirely.
- `filters.exclude_titles` — currently tuned to drop senior / staff / lead /
  L5+ wording; relax or invert if you're searching for those instead.
- `external_sources` — add more SimplifyJobs-schema feeds or write a new
  module under `monitor/external/`.

Then set the `NTFY_TOPIC` repo secret to an unguessable string, subscribe
your phone to that topic, and enable Actions. Setup notes are in
[`monitor/README.md`](monitor/README.md).

## Running locally

```sh
pip install -e .
pip install -r monitor/requirements.txt
export NTFY_TOPIC=your-topic-here
python -m monitor.run
```

Local runs fold in LinkedIn / Glassdoor / Bayt (which CI can't reach) and
append to the same `jobs.db`, so they dedupe against prior CI runs.

## Using JobSpy as a library

The underlying `jobspy` package is unchanged from upstream and still usable
on its own:

```python
from jobspy import scrape_jobs

jobs = scrape_jobs(
    site_name=["indeed", "linkedin", "glassdoor"],
    search_term="software engineer",
    location="London, United Kingdom",
    results_wanted=50,
    hours_old=72,
    country_indeed="UK",
)
print(jobs.head())
```

For the full upstream API surface — site list, parameters, country support,
schema — see the [upstream JobSpy README](https://github.com/cullenwatson/JobSpy)
or `pip install python-jobspy` for the published package.

## License

MIT, inherited from upstream JobSpy. See [LICENSE](LICENSE).
