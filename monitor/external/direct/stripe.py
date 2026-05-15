"""Stripe careers (Greenhouse-backed)."""

from __future__ import annotations

from monitor.external.direct import fetch_greenhouse, greenhouse_to_rows

SITE_LABEL = "direct:stripe"
BOARD_TOKEN = "stripe"
COMPANY = "Stripe"
COMPANY_URL = "https://stripe.com/"


def fetch_listings() -> list[dict]:
    jobs = fetch_greenhouse(BOARD_TOKEN)
    return greenhouse_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
