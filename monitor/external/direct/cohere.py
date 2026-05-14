"""Cohere careers (Ashby-backed)."""

from __future__ import annotations

from monitor.external.direct import ashby_to_rows, fetch_ashby

SITE_LABEL = "direct:cohere"
BOARD_NAME = "cohere"
COMPANY = "Cohere"
COMPANY_URL = "https://cohere.com/"


def fetch_listings() -> list[dict]:
    jobs = fetch_ashby(BOARD_NAME)
    return ashby_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
