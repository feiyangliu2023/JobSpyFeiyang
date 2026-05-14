"""Scale AI careers (Ashby-backed)."""

from __future__ import annotations

from monitor.external.direct import ashby_to_rows, fetch_ashby

SITE_LABEL = "direct:scaleai"
BOARD_NAME = "scaleai"
COMPANY = "Scale AI"
COMPANY_URL = "https://scale.com/"


def fetch_listings() -> list[dict]:
    jobs = fetch_ashby(BOARD_NAME)
    return ashby_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
