"""ntfy.sh push notifications.

One JSON-POST per run with title/tags/priority. We never spam: zero new jobs
means zero notifications.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from typing import Sequence

import requests


log = logging.getLogger(__name__)

NTFY_BASE_URL = os.environ.get("NTFY_BASE_URL", "https://ntfy.sh")
DEFAULT_TIMEOUT = 15


def _city_from_search_name(search_name: str | None) -> str:
    """`sde_junior_london` -> `london`. Falls back to the raw value."""
    if not search_name:
        return "?"
    parts = search_name.rsplit("_", 1)
    return parts[-1] if parts else search_name


def build_digest_body(new_jobs: Sequence[dict], top_n: int = 5) -> str:
    """Compose the message body: per-city counts plus the top N newest."""
    if not new_jobs:
        return ""

    counts = Counter(_city_from_search_name(j.get("search_name")) for j in new_jobs)
    counts_line = ", ".join(
        f"{city.title()}: {n}" for city, n in counts.most_common()
    )

    lines = [counts_line, ""]
    for j in new_jobs[:top_n]:
        title = (j.get("title") or "").strip() or "(no title)"
        company = (j.get("company") or "").strip() or "(unknown)"
        location = (j.get("location") or "").strip() or _city_from_search_name(
            j.get("search_name")
        ).title()
        url = (j.get("job_url") or "").strip()
        lines.append(f"- {title} @ {company} ({location})")
        if url:
            lines.append(f"  {url}")

    if len(new_jobs) > top_n:
        lines.append("")
        lines.append(f"...and {len(new_jobs) - top_n} more")

    return "\n".join(lines)


def send_digest(new_jobs: Sequence[dict], topic: str | None = None) -> bool:
    """Send the digest. Returns True if a request was attempted and accepted.

    Skips entirely (returns False) when there are no new jobs.
    """
    if not new_jobs:
        log.info("send_digest: no new jobs, skipping notification")
        return False

    topic = topic or os.environ.get("NTFY_TOPIC")
    if not topic:
        log.warning("send_digest: NTFY_TOPIC not set, skipping notification")
        return False

    body = build_digest_body(new_jobs)
    payload = {
        "topic": topic,
        "title": f"Job monitor: {len(new_jobs)} new",
        "message": body,
        "tags": ["briefcase"],
        "priority": 3,
    }

    try:
        resp = requests.post(NTFY_BASE_URL, json=payload, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        log.info("send_digest: sent %d-job digest to ntfy topic %s", len(new_jobs), topic)
        return True
    except requests.RequestException as e:
        log.error("send_digest: ntfy POST failed: %s", e)
        return False
