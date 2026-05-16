"""Per-source health accounting for one monitor run.

The point: tell the user — at the end of every run — *which sources are
healthy, which look blocked, and which are throwing errors*, without
making them grep through `run.log`.

Sources are tracked individually: `linkedin`, `indeed`, `glassdoor`,
`google`, `bayt` (the JobSpy site names) plus `external:simplify_newgrad`,
`external:simplify_intern` (whatever is configured in `external_sources`).

Status classifications (computed at end of run):

  - OK        — at least one call returned non-empty, no errors > 30% of attempts
  - DEGRADED  — some calls returned non-empty but success rate < 30%
                (suggests a flaky source or partial rate-limit)
  - SILENT    — at least `_SILENT_MIN_ATTEMPTS` calls all returned 0 rows
                AND no exceptions thrown (almost always means an IP
                block — nothing matched is statistically unlikely
                across that many searches). Below the threshold we
                classify as UNUSED instead, because naturally low-recall
                pairings (e.g. Bayt + applied_scientist, single-attempt
                direct scrapers) routinely return 0 on 1-2 calls and we
                don't want those to fire a SILENT ntfy alert.
  - BROKEN    — every call threw (URL down, schema changed, etc.)
  - UNUSED    — either never invoked this run (config flag,
                sites_skip_in_ci, etc.) OR invoked fewer than
                `_SILENT_MIN_ATTEMPTS` times with 0 rows and no errors
                (not enough signal to call a block).

The renderer of the report is intentionally text-only — it goes to
`run.log`, to `logs/health-latest.json` for grepability, and (if any
source is non-OK) gets pushed via `notify.send_health_alert`.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


_STATUS_GLYPH = {
    "OK": "OK ",
    "DEGRADED": "DEG",
    "SILENT": "SLT",
    "BROKEN": "BRK",
    "UNUSED": "—  ",
}

# Threshold below which a source with at least one success is still
# considered DEGRADED. 0.30 picked empirically — Google often returns
# 0 for niche search terms, so a 25% success rate is "this source has
# spotty matches, not broken".
_DEGRADED_RATE = 0.30

# Minimum attempts required before a 0-row, 0-error source can be
# classified as SILENT (i.e. "looks blocked"). Below this we fall back
# to UNUSED — single-attempt sources (per-company direct scrapers) and
# naturally low-recall pairings (Bayt + applied_scientist) routinely
# return 0 on 1-2 calls without being blocked, and we don't want those
# to trigger a SILENT ntfy alert every run. 3 picked as the smallest
# sample where "every call returned 0" starts to look statistically
# suspicious rather than expected noise.
_SILENT_MIN_ATTEMPTS = 3


@dataclass
class SourceStats:
    """Counters for one source over one monitor run."""

    attempts: int = 0           # how many times we called this source
    successes: int = 0          # calls that returned ≥1 row
    raw_rows: int = 0           # total rows scraped (pre-filter)
    filtered_rows: int = 0      # rows surviving apply_filters
    new_rows: int = 0           # rows new to jobs.db this run
    errors: int = 0             # exceptions caught
    error_samples: list[str] = field(default_factory=list)

    def success_rate(self) -> float:
        if self.attempts == 0:
            return 0.0
        return self.successes / self.attempts

    def status(self) -> str:
        if self.attempts == 0:
            return "UNUSED"
        if self.errors >= self.attempts:
            # Every call threw — source is broken at the protocol level
            return "BROKEN"
        if self.successes == 0:
            # No call succeeded but not every call threw — most likely
            # silent rate-limiting (LinkedIn classic pattern). Require
            # at least `_SILENT_MIN_ATTEMPTS` calls before we commit to
            # that classification, otherwise a naturally low-recall
            # source (single-call direct scraper, Bayt on niche queries)
            # would fire a false SILENT every time it returned 0.
            if self.attempts < _SILENT_MIN_ATTEMPTS:
                return "UNUSED"
            return "SILENT"
        if self.success_rate() < _DEGRADED_RATE:
            return "DEGRADED"
        return "OK"

    @property
    def status_glyph(self) -> str:
        return _STATUS_GLYPH[self.status()]


class HealthTracker:
    """Accumulates per-source stats during one monitor run.

    Pass an instance into `run_search` and `ingest_external_sources`;
    they call `record_*` methods as they go. At end of run, call
    `summary_lines()` for a log-friendly report and `write_json()` to
    persist to disk.
    """

    def __init__(self) -> None:
        self.sources: dict[str, SourceStats] = defaultdict(SourceStats)

    # ---- recording ------------------------------------------------------

    def record_attempt(self, source: str) -> None:
        self.sources[source].attempts += 1

    def record_outcome(self, source: str, raw_rows: int) -> None:
        """One call returned (without throwing). `raw_rows` may be 0."""
        s = self.sources[source]
        s.raw_rows += raw_rows
        if raw_rows > 0:
            s.successes += 1

    def record_filtered(self, source: str, n: int) -> None:
        self.sources[source].filtered_rows += n

    def record_new(self, source: str, n: int) -> None:
        self.sources[source].new_rows += n

    def record_error(self, source: str, exc: BaseException) -> None:
        s = self.sources[source]
        s.errors += 1
        msg = f"{type(exc).__name__}: {exc}"[:300]
        if msg not in s.error_samples and len(s.error_samples) < 3:
            s.error_samples.append(msg)

    # ---- reporting ------------------------------------------------------

    def overall_status(self) -> str:
        """Worst per-source status, with a fixed precedence.

        Special case: if AT LEAST ONE source returned OK we don't escalate
        all the way to BROKEN/SILENT — the run still produced data, even
        if some sites got blocked (steady state for CI: indeed +
        SimplifyJobs OK, glassdoor/google/bayt SILENT). In that case we
        downgrade to DEGRADED so the run exits 0 (caller treats DEGRADED
        as non-fatal) but the per-source health alert still fires for
        the SILENT sources. Only when ZERO sources are OK do we surface
        BROKEN/SILENT, which is what genuinely warrants a red CI run.
        """
        statuses = [s.status() for s in self.sources.values() if s.attempts > 0]
        if not statuses:
            return "UNUSED"
        any_ok = any(st == "OK" for st in statuses)
        if any_ok:
            for tier in ("BROKEN", "SILENT", "DEGRADED"):
                if tier in statuses:
                    return "DEGRADED"
            return "OK"
        for tier in ("BROKEN", "SILENT", "DEGRADED"):
            if tier in statuses:
                return tier
        return "OK"

    def has_warnings(self) -> bool:
        return self.overall_status() in ("BROKEN", "SILENT", "DEGRADED")

    def summary_lines(self) -> list[str]:
        """Aligned table for `log.info` consumption."""
        if not self.sources:
            return ["[health] no source activity recorded this run"]

        # Compute column widths from data so the table doesn't wrap on
        # short or unusually long source names.
        name_width = max(22, max(len(n) for n in self.sources) + 2)

        lines: list[str] = []
        lines.append("=" * 78)
        lines.append("[health] per-source report:")
        header = (
            f"  {'source':<{name_width}} {'st':<3}  "
            f"{'attempts':>8}  {'succ':>4}  {'raw':>6}  {'filt':>4}  {'new':>4}"
        )
        lines.append(header)
        lines.append("  " + "-" * (len(header) - 2))

        for name in sorted(self.sources):
            s = self.sources[name]
            lines.append(
                f"  {name:<{name_width}} {s.status_glyph}  "
                f"{s.attempts:>8}  {s.successes:>4}  {s.raw_rows:>6}  "
                f"{s.filtered_rows:>4}  {s.new_rows:>4}"
            )
            if s.errors:
                lines.append(
                    f"      {s.errors} error(s); first sample(s):"
                )
                for err in s.error_samples:
                    lines.append(f"        {err}")

        overall = self.overall_status()
        lines.append("")
        lines.append(f"  overall: {overall}")
        if overall == "SILENT":
            lines.append(
                "  hint: SILENT sources usually mean an IP block. "
                "Try setting JOBSPY_PROXIES or running locally."
            )
        elif overall == "BROKEN":
            lines.append(
                "  hint: BROKEN sources had every call throw — check the "
                "error samples above. Schema change upstream is the usual cause."
            )
        lines.append("=" * 78)
        return lines

    def to_dict(self) -> dict:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "overall": self.overall_status(),
            "sources": {
                name: {
                    **asdict(stats),
                    "status": stats.status(),
                    "success_rate": round(stats.success_rate(), 3),
                }
                for name, stats in sorted(self.sources.items())
            },
        }

    def write_json(self, log_dir: str | Path) -> Path:
        """Persist `to_dict()` as `logs/health-latest.json` (overwrites)."""
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        out_path = log_dir / "health-latest.json"
        out_path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return out_path

    def alert_lines(self) -> list[str]:
        """Compact list of bad sources for ntfy push.

        Returns [] when nothing is wrong — caller can use this as a
        guard for "should I send an alert?".
        """
        lines: list[str] = []
        for name in sorted(self.sources):
            s = self.sources[name]
            st = s.status()
            if st in ("BROKEN", "SILENT", "DEGRADED"):
                bits = [f"{name}: {st}"]
                bits.append(f"{s.successes}/{s.attempts} calls")
                if s.errors:
                    bits.append(f"{s.errors} err")
                if s.new_rows:
                    bits.append(f"{s.new_rows} new")
                lines.append(" — ".join(bits))
        return lines

    def failed_sources(self) -> list[tuple[str, str]]:
        """Return [(name, status)] for every BROKEN / SILENT source.

        Distinct from `alert_lines` in two ways: (1) it returns
        structured tuples instead of pre-formatted strings, so callers
        can route to different log levels per status; (2) it excludes
        DEGRADED, which counts as partial-success — the end-of-run
        "what failed" log is reserved for sources that produced zero
        usable data so the user can see at a glance which scrapers
        need attention without scanning the full status table.
        """
        out: list[tuple[str, str]] = []
        for name in sorted(self.sources):
            s = self.sources[name]
            st = s.status()
            if st in ("BROKEN", "SILENT"):
                out.append((name, st))
        return out

    def failed_sources_lines(self) -> list[str]:
        """End-of-run failure block printed verbatim to the log.

        Returns a self-contained banner (header + per-source detail +
        footer) listing every source that the run could not extract
        data from. Empty list means nothing failed — caller can use
        `if lines:` to gate the print.

        Per-source line shape:
            <name>  [<STATUS>]  attempts=N successes=0 errors=M
              error: <first sample, truncated>
        """
        failed = self.failed_sources()
        if not failed:
            return []
        lines: list[str] = []
        lines.append("=" * 78)
        lines.append(f"[health] FAILED SOURCES THIS RUN ({len(failed)}):")
        for name, status in failed:
            s = self.sources[name]
            lines.append(
                f"  - {name}  [{status}]  attempts={s.attempts} "
                f"successes={s.successes} errors={s.errors}"
            )
            for err in s.error_samples:
                lines.append(f"      error: {err}")
        lines.append("=" * 78)
        return lines
