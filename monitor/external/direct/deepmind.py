"""Google DeepMind careers — intentionally not implemented.

DeepMind doesn't run its own ATS; postings live on Google Careers
(`google.com/about/careers/applications/`), which is a single-page React
app gated behind Google's anti-bot infrastructure. The public JSON
endpoint that backs it (`googleapis.com/.../jobs.search`) requires API
keys baked into the page bundle plus a session cookie, and rate-limits
aggressively even with valid credentials.

For DeepMind coverage we already have two working channels — leaving
this stub in place documents the deliberate choice so the next person
(or session) doesn't re-attempt the scrape:

  1. **SimplifyJobs feeds** (`simplify_newgrad`, `simplify_intern`) —
     maintainers manually add DeepMind / Google postings. Lag is ~1-3
     days but the data is clean.
  2. **JobSpy via Indeed/LinkedIn** — title + "google deepmind" company
     filter on the EMEA cities catches London/Zurich/Paris roles. The
     allowlist already routes "deepmind, google deepmind" through.

If the user later wants direct coverage anyway, the realistic options
are: (a) Playwright/Selenium driving careers.google.com (slow, fragile,
needs a headless browser dependency), or (b) RSS feeds aggregated by
third parties (theyseekyou.com etc — adds another point of lag).
Neither is worth the maintenance cost given (1) and (2).
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def fetch_listings() -> list[dict]:
    """No-op. See module docstring for the rationale."""
    log.info(
        "direct.deepmind: stub — DeepMind coverage comes from SimplifyJobs "
        "+ JobSpy. See module docstring."
    )
    return []
