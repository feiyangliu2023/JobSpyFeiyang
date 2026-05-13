"""Anthropic careers (Greenhouse-backed).

Anthropic's public board lives at `anthropic.com/jobs` and is rendered
from Greenhouse. We hit the structured JSON feed directly:

    https://boards-api.greenhouse.io/v1/boards/anthropic/jobs

which returns the entire active job list (~400 rows) in one response, no
auth required. Faster than scraping the rendered HTML and stable across
front-end redesigns.

Junior/new-grad title filtering happens in the global filter chain
(`apply_filters` with `exclude_titles` + `include_title_keywords`) — this
module does no scraper-side title gating, mirroring `simplify.py`.
"""

from __future__ import annotations

from monitor.external.direct import fetch_greenhouse, greenhouse_to_rows

SITE_LABEL = "direct:anthropic"
BOARD_TOKEN = "anthropic"
COMPANY = "Anthropic"
COMPANY_URL = "https://www.anthropic.com/"


def fetch_listings() -> list[dict]:
    """Return Anthropic job listings as jobs.db rows."""
    jobs = fetch_greenhouse(BOARD_TOKEN)
    return greenhouse_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
