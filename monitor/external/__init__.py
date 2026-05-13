"""External (non-JobSpy) job sources that feed into the same jobs.db.

This module also hosts the shared HTTP plumbing every external scraper
uses — a header builder, a bounded retry helper, and an
ETag / Last-Modified file cache. Keeping it in one place means:

  - simplify.py and direct/*.py share the same UA-rotation pool and
    retry shape, so a transient blip on any source no longer tanks
    the run and triggers a noisy SILENT ntfy alert.
  - Header rotation lives in one builder rather than being copy-pasted
    into every scraper (where each one would inevitably drift).
  - ETag / Last-Modified is a single code path the direct scrapers
    plug into without each one re-implementing the cache file format.

WHY a small UA pool rather than `jobspy-monitor/1.0`: a single fixed
non-browser UA is the cheapest "this is a scraper" signal a WAF can
key off. Rotating across a handful of recent Chromes plus a real
Accept-Language and (where applicable) a careers-page Referer raises
the floor enough to dodge the most basic UA-filter rules without
pretending to be a stealth scraper.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path

import requests

log = logging.getLogger(__name__)


# Cache directory for ETag / Last-Modified state. One JSON file per
# source (`greenhouse_<board>.json`, `ashby_<board>.json`) so a 304
# from one feed doesn't invalidate another. Gitignored — these files
# are pure HTTP cache, not data the repo needs to track.
CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"


# UA rotation pool. Real recent Chrome releases on Windows / macOS /
# Linux. We rotate per-call (random.choice) so multiple direct scrapers
# hitting the same ATS don't all wear identical fingerprints. Kept
# small intentionally — picking from 4 plausible UAs is enough to dodge
# trivial UA-block rules without giving the impression of evasion.
_UA_POOL = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
)


# Retry schedule: 3 attempts total at 2s / 5s / 10s. The last entry is
# never slept (we re-raise after the final failure) — it just sets the
# upper bound a future expansion would land at. Bounded on purpose:
# this is a twice-daily cron, so a hard block should fail FAST and let
# the health report flag it, not extend the run by minutes of backoff.
_RETRY_DELAYS = (2.0, 5.0, 10.0)


# Status codes we treat as transient. 429 = rate-limited (back off and
# retry), 5xx = upstream wobble. 4xx other than 429 (403, 404, etc.)
# are NOT retried — those are usually permanent signals (block, gone)
# and retrying just burns budget.
_TRANSIENT_STATUSES = frozenset({429, 500, 502, 503, 504})


def _http_debug() -> bool:
    """JOBSPY_HTTP_DEBUG=1 → log first ~500 bytes of any non-2xx body."""
    return os.environ.get("JOBSPY_HTTP_DEBUG", "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def build_headers(*, referer: str | None = None) -> dict[str, str]:
    """Build a per-request header dict.

    Rotates across `_UA_POOL`, sets a real `Accept-Language`, and adds
    `Referer` when the caller supplies one (typically the company's
    careers page URL). See module docstring for the WHY.
    """
    headers = {
        "User-Agent": random.choice(_UA_POOL),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
    return headers


@dataclass
class HttpResult:
    """Result of one `http_get` call.

    `cached=True` means the server returned 304 against our stored
    ETag / Last-Modified — caller should serve from its own cache.
    `body` is None in that case (no body shipped on 304).
    """

    status: int
    body: bytes | None
    etag: str | None
    last_modified: str | None
    cached: bool = False


def read_cache(cache_key: str) -> dict | None:
    """Load the cache JSON for `cache_key`. None on miss / read error."""
    path = CACHE_DIR / f"{cache_key}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        log.warning("cache read failed for %s: %s", cache_key, e)
        return None


def write_cache(cache_key: str, payload: dict) -> None:
    """Persist `payload` (any JSON-serializable dict) under `cache_key`.

    Best-effort: cache write failures are logged but don't propagate —
    the worst case is an extra full GET next run.
    """
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = CACHE_DIR / f"{cache_key}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError as e:
        log.warning("cache write failed for %s: %s", cache_key, e)


def http_get(
    url: str,
    *,
    source_label: str,
    headers: dict[str, str] | None = None,
    timeout: int = 60,
    cache_key: str | None = None,
    max_attempts: int = 3,
) -> HttpResult:
    """GET `url` with bounded retry + optional If-None-Match handling.

    Retries on connection errors, 5xx, and 429 — three attempts at
    2s / 5s / 10s. Other 4xx (403, 404) surface immediately because
    retrying a hard block just delays the inevitable.

    When `cache_key` is set, read `<CACHE_DIR>/<key>.json` for a stored
    ETag / Last-Modified and send them as `If-None-Match` /
    `If-Modified-Since`. A 304 response returns
    `HttpResult(cached=True, body=None, ...)`; the caller is responsible
    for keeping its own parsed-data cache and pulling from it on 304.
    This helper does NOT write the cache file — only the caller knows
    what payload should accompany the new ETag / Last-Modified.

    On final failure the underlying exception propagates; callers wrap
    this in their own try/except so one source's outage doesn't kill
    the run (mirrors the existing scraper pattern).
    """
    headers = dict(headers or {})

    cache = read_cache(cache_key) if cache_key else None
    if cache:
        etag = cache.get("etag")
        last_mod = cache.get("last_modified")
        if etag and "If-None-Match" not in headers:
            headers["If-None-Match"] = etag
        if last_mod and "If-Modified-Since" not in headers:
            headers["If-Modified-Since"] = last_mod

    for attempt in range(max_attempts):
        is_last = attempt == max_attempts - 1
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as e:
            if is_last:
                raise
            delay = _RETRY_DELAYS[attempt]
            log.warning(
                "%s: connection error on attempt %d/%d (%s); retrying in %.1fs",
                source_label, attempt + 1, max_attempts, e, delay,
            )
            time.sleep(delay)
            continue

        status = resp.status_code

        if status == 304:
            return HttpResult(
                status=304,
                body=None,
                etag=(cache or {}).get("etag"),
                last_modified=(cache or {}).get("last_modified"),
                cached=True,
            )

        if 200 <= status < 300:
            return HttpResult(
                status=status,
                body=resp.content,
                etag=resp.headers.get("ETag"),
                last_modified=resp.headers.get("Last-Modified"),
            )

        if _http_debug():
            try:
                snippet = resp.text[:500]
            except Exception:
                snippet = "<unreadable>"
            log.info(
                "[http-debug] %s %d for %s: %s",
                source_label, status, url, snippet,
            )

        if status in _TRANSIENT_STATUSES and not is_last:
            delay = _RETRY_DELAYS[attempt]
            log.warning(
                "%s: HTTP %d on attempt %d/%d; retrying in %.1fs",
                source_label, status, attempt + 1, max_attempts, delay,
            )
            time.sleep(delay)
            continue

        resp.raise_for_status()
        # Unreachable for non-2xx (raise_for_status threw above), but
        # keep the return for static type checkers.
        return HttpResult(
            status=status,
            body=resp.content,
            etag=resp.headers.get("ETag"),
            last_modified=resp.headers.get("Last-Modified"),
        )

    # Loop only exits via return/raise above; this is a defensive backstop.
    raise RuntimeError(f"{source_label}: http_get exhausted retries")
