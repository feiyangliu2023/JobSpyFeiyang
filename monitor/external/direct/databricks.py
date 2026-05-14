"""Databricks careers (Greenhouse-backed)."""

from __future__ import annotations

from monitor.external.direct import fetch_greenhouse, greenhouse_to_rows

SITE_LABEL = "direct:databricks"
BOARD_TOKEN = "databricks"
COMPANY = "Databricks"
COMPANY_URL = "https://www.databricks.com/"


def fetch_listings() -> list[dict]:
    jobs = fetch_greenhouse(BOARD_TOKEN)
    return greenhouse_to_rows(
        jobs,
        site_label=SITE_LABEL,
        company=COMPANY,
        company_url=COMPANY_URL,
    )
