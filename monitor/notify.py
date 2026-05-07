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


def send_health_alert(health, topic: str | None = None) -> bool:
    """Push a per-source health alert to ntfy when any source is non-OK.

    Returns True only if a non-empty payload was actually accepted by
    the server. Returns False (and does nothing else) when:
      - there's nothing wrong (`health.has_warnings()` is False)
      - `NTFY_TOPIC` isn't configured
      - `NTFY_HEALTH_ALERTS` env var is set to a falsy value
        ("0", "off", "false", "no")

    The alert priority is bumped to 4 (above the digest's 3) and
    tagged with `warning` + `construction` so the user's phone treats
    it as "something needs attention" rather than "new jobs in your
    inbox".
    """
    if not health.has_warnings():
        return False

    raw_toggle = os.environ.get("NTFY_HEALTH_ALERTS", "1").strip().lower()
    if raw_toggle in ("0", "off", "false", "no", "disabled"):
        log.info("send_health_alert: disabled via NTFY_HEALTH_ALERTS env var")
        return False

    topic = topic or os.environ.get("NTFY_TOPIC")
    if not topic:
        log.warning("send_health_alert: NTFY_TOPIC not set, skipping notification")
        return False

    overall = health.overall_status()
    body_lines = health.alert_lines()
    body = "\n".join(body_lines) if body_lines else "(no detail)"

    payload = {
        "topic": topic,
        "title": f"[monitor] {overall}: source(s) need attention",
        "message": body,
        "tags": ["warning", "construction"],
        "priority": 4,
    }

    try:
        resp = requests.post(NTFY_BASE_URL, json=payload, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        log.info(
            "send_health_alert: posted %s alert to ntfy topic %s (%d affected sources)",
            overall, topic, len(body_lines),
        )
        return True
    except requests.RequestException as e:
        log.error("send_health_alert: ntfy POST failed: %s", e)
        return False
