# Job monitor

Twice-daily scheduled scrape of EMEA junior-level SDE / MLE / Applied Scientist
roles at large tech companies. Net-new postings are pushed as a single digest
to your phone via [ntfy.sh](https://ntfy.sh).

Runs in GitHub Actions on a cron, persists state in `jobs.db` (committed back
to the repo on each run), and falls back to local execution when GitHub
Actions IPs get blocked.

## Setup

1. **Pick an ntfy topic.** Any string works — make it long and unguessable so
   nobody else can subscribe. Example: `jobs-feiyang-7xq2k9p`. There's no
   account; the topic *is* your authentication.
2. **Set the secret.** In your fork on GitHub: *Settings → Secrets and
   variables → Actions → New repository secret* → name `NTFY_TOPIC`, value
   your topic string.
3. **Enable Actions.** *Actions* tab → enable workflows. The `scrape.yml`
   workflow will then run on its cron and on manual dispatch.
4. **Subscribe on your phone.** Install the ntfy app
   ([iOS](https://apps.apple.com/us/app/ntfy/id1625396347) /
   [Android](https://play.google.com/store/apps/details?id=io.heckel.ntfy))
   and *Add subscription* → enter your topic name. No login.

## Tuning

Edit `monitor/config.yaml`:

- `cities` — add/remove EMEA hubs. Each entry needs `name` (used in the
  search bucket name), `location` (the free-text string passed to JobSpy),
  and `country_indeed` (a country name from `jobspy.model.Country`).
- `role_templates` — each template becomes a search bucket per city. Add
  more `search_terms` to broaden coverage; they all upsert into the same
  bucket so you don't get duplicate notifications.
- `filters.include_companies` — the allowlist. Synonyms can be nested as
  inner lists (e.g. `[tiktok, bytedance]`). Matching is case-insensitive
  substring — "google" matches "Google LLC".
- `filters.exclude_titles` — case-insensitive substring on the title. The
  pre-populated set catches senior / staff / lead / level-5+ wording.
- `filters.min_description_chars` — drops suspiciously empty postings.

## Trigger manually

*Actions tab → Job monitor scrape → Run workflow*. Useful right after editing
the config to verify the new search shape before waiting for cron.

## Inspect the DB locally

```sh
sqlite3 monitor/jobs.db
sqlite> .schema
sqlite> SELECT count(*) FROM jobs WHERE status = 'active';
sqlite> SELECT title, company, location, first_seen
        FROM jobs ORDER BY first_seen DESC LIMIT 20;
sqlite> SELECT search_name, sum(rows_scraped), sum(rows_new)
        FROM runs WHERE started_at > date('now','-7 day')
        GROUP BY search_name ORDER BY 3 DESC;
```

## Run locally (recommended fallback)

LinkedIn blocks GitHub Actions IP ranges aggressively, so the CI run will
often see sparse or zero LinkedIn results. Indeed and Glassdoor still work
reliably. To fill the LinkedIn gap from your home IP:

```sh
pip install -e .
pip install -r monitor/requirements.txt
export NTFY_TOPIC=your-topic-here   # (PowerShell: $env:NTFY_TOPIC = "...")
python -m monitor.run
```

This will append to the same `jobs.db`, so your local run dedupes against the
last CI run.

## What to expect

- Cron runs at **07:00 UTC** and **15:00 UTC** (catches EMEA morning posts
  and the afternoon batch). Adjust in `.github/workflows/scrape.yml`.
- Each successful run commits the updated `jobs.db` and `JOBS.md` back to
  the branch with a `[skip ci]` message so it doesn't trigger itself.
- **`JOBS.md`** at the repo root renders the current active jobs as a
  GitHub-friendly table grouped into FAANG+ / Quant / Other tiers, with
  per-row Apply buttons. It overwrites every run so age counters tick up
  and gone roles disappear.
- Zero net-new jobs = no ntfy notification (no spam).
- On failure, the workflow uploads `monitor/logs/` as a build artifact —
  download it from the run page to see what broke.
