"""Scale AI careers (Greenhouse-backed).

Originally wired up with Ashby + slug `scaleai` (PR #8 — that ATS was a
guess from the sandbox where the relevant CDNs are firewalled). The
first scheduled CI run with the source live returned 0 rows: Scale AI's
careers page actually posts via Greenhouse (apply URLs route through
`job-boards.greenhouse.io/scaleai/...`), so the Ashby endpoint either
404'd or returned an empty / off-tenant payload.
"""

from __future__ import annotations

from monitor.external.direct import fetch_greenhouse, greenhouse_to_rows

SITE_LABEL = "direct:scaleai"
BOARD_TOKEN = "scaleai"
COMPANY = "Scale AI"
COMPANY_URL = "https://scale.com/"


def fetch_listings() -> list[dict]:
    jobs = fetch_greenhouse(BOARD_TOKEN)
    return greenhouse_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
