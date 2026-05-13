"""OpenAI careers (Ashby-backed).

OpenAI's careers page (`openai.com/careers`) is hosted on Ashby. The
public posting-api feed lives at:

    https://api.ashbyhq.com/posting-api/job-board/openai

Returns the full active board (~600 rows) in one JSON response, no auth.

We use `applyUrl` rather than `jobUrl` so the rendered "Apply" link in
JOBS.md drops the user directly on the application form rather than the
listing page. `isListed=false` rows (Ashby's hidden flag) are dropped.

Junior/new-grad filtering is left to the global `apply_filters` block.
"""

from __future__ import annotations

from monitor.external.direct import ashby_to_rows, fetch_ashby

SITE_LABEL = "direct:openai"
BOARD_NAME = "openai"
COMPANY = "OpenAI"
COMPANY_URL = "https://openai.com/"


def fetch_listings() -> list[dict]:
    """Return OpenAI job listings as jobs.db rows."""
    jobs = fetch_ashby(BOARD_NAME)
    return ashby_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
