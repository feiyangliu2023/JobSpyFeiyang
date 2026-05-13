"""Batch-check the job_url for every active row so dead apply links don't
sit in slice files for weeks.

Pattern: HEAD first (cheap), fall back to streamed GET when the server
rejects HEAD with 403/405. For known job-board domains a 200 response
isn't enough — Indeed and LinkedIn redirect dead postings to a search
results page (status 200 with totally different content), and SimplifyJobs
returns 200 on a "Job not found" stub. Those quirks are encoded as
post-redirect URL patterns + a body-sniff fallback.

Threading: parallelism via ThreadPoolExecutor (max_workers=8). DB reads
+ writes happen on the main thread; workers only do HTTP. A per-domain
rate limiter (~20 req/min/domain) prevents getting the scraper IP banned
mid-check.
"""

from __future__ import annotations

import logging
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests


log = logging.getLogger("monitor.liveness")


# --------------------------------------------------------------------------- #
# Per-domain rate limiting
# --------------------------------------------------------------------------- #

# 20 requests / minute / domain = one request every 3s. The 0.5-2s random
# jitter on top spreads burst patterns so we don't look like a poller.
_DOMAIN_MIN_INTERVAL_S = 3.0
_DOMAIN_JITTER_RANGE = (0.5, 2.0)

# {domain: time-after-which-the-domain-is-free}. We update this under the
# lock to reserve a slot BEFORE sleeping, so two workers arriving
# simultaneously queue rather than colliding.
_domain_reserved_until: dict[str, float] = {}
_domain_lock = threading.Lock()


def _domain_of(url: str) -> str:
    """Lowercase hostname with leading 'www.' stripped — so `www.foo.com`
    and `foo.com` share the same rate-limit bucket."""
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _rate_limited_sleep(domain: str) -> None:
    """Block until this domain's next slot is free, then reserve it.

    The reservation pattern (update the dict BEFORE sleeping) ensures
    two workers don't both see `last=0` and fire back-to-back. With 8
    workers piling onto one domain, requests serialize cleanly at ~3s
    intervals.
    """
    if not domain:
        time.sleep(random.uniform(*_DOMAIN_JITTER_RANGE))
        return
    with _domain_lock:
        now = time.time()
        free_at = _domain_reserved_until.get(domain, 0.0)
        start = max(now, free_at)
        jitter = random.uniform(*_DOMAIN_JITTER_RANGE)
        slot_end = start + _DOMAIN_MIN_INTERVAL_S + jitter
        _domain_reserved_until[domain] = slot_end
        wait = start - now + jitter
    if wait > 0:
        time.sleep(wait)


# --------------------------------------------------------------------------- #
# URL check
# --------------------------------------------------------------------------- #

_DEFAULT_TIMEOUT = 10
_BODY_PEEK_BYTES = 50 * 1024  # cap body sniffing at 50KB

# A real browser UA. Some job boards 403 the python-requests default UA
# outright (LinkedIn most notably) — using a recent Chrome string gets
# us a normal response.
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Post-redirect URL patterns that mean "this posting is gone" for a given
# origin host. The check is: origin appears in the requested URL AND the
# marker substring appears in the final (post-redirect) URL.
#
# Examples in the wild:
#   indeed.com/viewjob?jk=abc123 → /q-software-engineer-jobs.html
#   indeed.com/cmp/Foo/jobs?...  → /jobs?q=foo (a search results page)
#   linkedin.com/jobs/view/123…  → /jobs/search/?keywords=...
_DEAD_REDIRECT_PATTERNS: list[tuple[str, str]] = [
    ("indeed.com", "/q-"),
    ("indeed.com", "/jobs?"),
    ("indeed.com", "/jobs.html"),
    ("linkedin.com", "/jobs/search"),
    ("linkedin.com", "/jobs/view/0"),
    ("glassdoor.", "/Job/jobs.htm"),
]

# Domains that return 200 with a "not found" page rather than 404. For
# these we do a small streamed GET (capped at _BODY_PEEK_BYTES) and look
# for the marker string in the body.
_BODY_SNIFF_DOMAINS: dict[str, tuple[str, ...]] = {
    "simplify.jobs": ("Job not found", "Position not found", "no longer available"),
}


def check_url(url: str, timeout: int = _DEFAULT_TIMEOUT) -> dict:
    """Check a single URL. Returns {status, code, final_url}.

    Status vocabulary matches the jobs.liveness_status column:
      - 'ok'       — 2xx/3xx and not a known "dead" redirect pattern.
      - '404'      — 404/410, or a 200 page identified as dead via
                     redirect pattern or body sniff.
      - 'redirect' — used when the dead signal came from a redirect-URL
                     pattern (rather than a literal 404). Kept distinct
                     from '404' so we can audit later: redirect-based
                     dead detection is heuristic; literal 404 is not.
      - 'timeout'  — connection / read timeout.
      - 'error'    — any other request failure, including 4xx other than
                     404/410 and any 5xx (transient — we retry next cycle).
    """
    headers = {"User-Agent": _UA, "Accept": "*/*"}
    final_url = url
    try:
        resp = requests.head(
            url,
            headers=headers,
            allow_redirects=True,
            timeout=timeout,
        )
        # 405 = Method Not Allowed (server disallows HEAD entirely),
        # 403 = some bot-detection paths only block HEAD. Fall back to
        # GET in both cases. stream=True so we don't pay full-body cost.
        if resp.status_code in (403, 405):
            resp.close()
            resp = requests.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=timeout,
                stream=True,
            )
        code = resp.status_code
        final_url = resp.url or url
        resp.close()
    except requests.Timeout:
        return {"status": "timeout", "code": None, "final_url": url}
    except requests.RequestException as e:
        log.debug("liveness error for %s: %s", url, e)
        return {"status": "error", "code": None, "final_url": url}

    if code in (404, 410):
        return {"status": "404", "code": code, "final_url": final_url}
    if code >= 500:
        # 5xx is almost always transient — bucket as error so we retry
        # next cycle rather than flipping the row to 'gone'.
        return {"status": "error", "code": code, "final_url": final_url}
    if code >= 400:
        # 4xx that isn't 404/410 (401/403/etc) — could be auth-gated; not
        # a confident "dead" signal so leave the row visible.
        return {"status": "error", "code": code, "final_url": final_url}

    # 2xx/3xx: inspect for known dead-redirect patterns and body markers.
    req_l = url.lower()
    final_l = final_url.lower()
    for origin, dead_marker in _DEAD_REDIRECT_PATTERNS:
        if origin in req_l and dead_marker in final_l:
            return {"status": "redirect", "code": code, "final_url": final_url}

    host = _domain_of(final_url)
    for snip_host, markers in _BODY_SNIFF_DOMAINS.items():
        if snip_host in host:
            try:
                body_resp = requests.get(
                    final_url,
                    headers=headers,
                    timeout=timeout,
                    stream=True,
                )
                chunks: list[bytes] = []
                total = 0
                for chunk in body_resp.iter_content(chunk_size=8192):
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= _BODY_PEEK_BYTES:
                        break
                body_resp.close()
                body = b"".join(chunks).decode("utf-8", errors="ignore").lower()
                for m in markers:
                    if m.lower() in body:
                        return {
                            "status": "404",
                            "code": code,
                            "final_url": final_url,
                        }
            except requests.RequestException:
                pass  # body sniff is best-effort; fall through to 'ok'
            break

    return {"status": "ok", "code": code, "final_url": final_url}


def _check_one(url: str, timeout: int) -> tuple[str, dict]:
    """Worker entry: respect per-domain pacing, then check."""
    _rate_limited_sleep(_domain_of(url))
    return url, check_url(url, timeout=timeout)


# --------------------------------------------------------------------------- #
# Batch checking against jobs.db
# --------------------------------------------------------------------------- #


def check_active_urls(
    conn: sqlite3.Connection,
    batch_size: int = 200,
    max_age_hours: int = 72,
    timeout: int = _DEFAULT_TIMEOUT,
    max_workers: int = 8,
) -> dict:
    """Recheck active jobs whose liveness data is stale.

    Pulls up to `batch_size` rows with `status='active'` whose
    `liveness_checked_at` is NULL or older than `max_age_hours`,
    ordered so NULLs (never checked) come first, then oldest-checked.
    This bounds per-run cost and ensures a fresh DB gets caught up
    over a few cycles rather than hammering everything at once.

    Side effects:
      - Updates `liveness_status`, `liveness_status_code`,
        `liveness_checked_at` for every row checked.
      - Flips `status='gone'` for any row that returned '404' (literal
        404/410 or body-sniffed "not found"). 'redirect' results are
        recorded but NOT auto-gone'd — the redirect heuristic is fuzzier
        than a literal 404 so we let the renderer hide it instead.

    Returns {checked, dead, errored}. `dead` counts rows flipped to gone
    this call; `errored` counts timeout+error outcomes (retried later).
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    ).isoformat(timespec="seconds")
    # `ORDER BY liveness_checked_at` puts NULLs first in SQLite (NULL
    # sorts smaller than any value by default), which is what we want —
    # never-checked rows take priority.
    rows = conn.execute(
        """
        SELECT job_url
          FROM jobs
         WHERE status = 'active'
           AND (liveness_checked_at IS NULL OR liveness_checked_at < ?)
         ORDER BY liveness_checked_at
         LIMIT ?
        """,
        (cutoff, batch_size),
    ).fetchall()
    urls = [r["job_url"] for r in rows if r["job_url"]]
    if not urls:
        log.info(
            "[liveness] nothing to check "
            "(batch_size=%d, max_age=%dh — every active URL is fresh)",
            batch_size, max_age_hours,
        )
        return {"checked": 0, "dead": 0, "errored": 0}

    log.info(
        "[liveness] checking %d URLs (batch_size=%d, max_age=%dh, workers=%d)",
        len(urls), batch_size, max_age_hours, max_workers,
    )

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_check_one, u, timeout) for u in urls]
        for fut in as_completed(futures):
            try:
                url, res = fut.result()
                results[url] = res
            except Exception as e:
                # A worker exception shouldn't kill the batch — just log
                # and skip. The unchecked URL will be picked up next run.
                log.warning("[liveness] worker exception: %s", e)

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    dead = 0
    errored = 0
    redirected = 0
    for url, res in results.items():
        status = res.get("status")
        code = res.get("code")
        conn.execute(
            """
            UPDATE jobs
               SET liveness_status      = ?,
                   liveness_status_code = ?,
                   liveness_checked_at  = ?
             WHERE job_url = ?
            """,
            (status, code, now_iso, url),
        )
        if status == "404":
            conn.execute(
                "UPDATE jobs SET status = 'gone' "
                "WHERE job_url = ? AND status = 'active'",
                (url,),
            )
            dead += 1
        elif status == "redirect":
            redirected += 1
        elif status in ("error", "timeout"):
            errored += 1
    conn.commit()

    log.info(
        "[liveness] done: checked=%d, dead=%d, redirect=%d, errored=%d",
        len(results), dead, redirected, errored,
    )
    return {
        "checked": len(results),
        "dead": dead,
        "redirected": redirected,
        "errored": errored,
    }


# --------------------------------------------------------------------------- #
# CLI smoke test — `python -m monitor.liveness <url> [<url> ...]`
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 2:
        print("usage: python -m monitor.liveness <url> [<url> ...]")
        raise SystemExit(2)
    for arg in sys.argv[1:]:
        out = check_url(arg)
        print(f"{out['status']:<8} {str(out['code'] or '-'):<5} {arg}")
        if out["final_url"] != arg:
            print(f"  → {out['final_url']}")
