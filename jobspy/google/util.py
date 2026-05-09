import json
import re

from jobspy.util import create_logger

log = create_logger("Google")

# Google's job-payload wrapper is keyed by an internal function ID
# that gets rotated occasionally — `520084652` worked for months, but
# any redeploy can flip it. Each candidate is tried in order; first
# one that yields a parseable list wins.
#
# To extend: add the new ID at the front of this list. Discover new
# IDs by saving the raw response from `_get_initial_cursor_and_jobs`
# and grepping for `\d{9}":\[\["` near a job title — the surrounding
# 9-digit key is what we need.
_KNOWN_JOB_KEYS = ("520084652",)


def _looks_like_job_record(item) -> bool:
    """Heuristic: does this list item look like a Google Jobs entry?

    A job record is a long list whose first three positions are
    strings (title, company, location) and which contains a nested
    list with a job URL several positions in. Used as a fallback
    when none of the known wrapper keys appears in the response.
    """
    if not isinstance(item, list) or len(item) < 13:
        return False
    if not all(isinstance(x, str) for x in item[:3]):
        return False
    title, company, location = item[0], item[1], item[2]
    if not (title and company and location):
        return False
    # The URL is at index 3 → first nested list, first item.
    try:
        url = item[3][0][0]
    except (IndexError, TypeError):
        return False
    return isinstance(url, str) and url.startswith("http")


def find_job_info(jobs_data: list | dict) -> list | None:
    """Recursively locate the job-listing array inside Google's JSON.

    First pass: look for one of the known wrapper keys (fast path,
    matches today's response shape). Second pass: fall back to a
    structural heuristic so a key rotation doesn't immediately make
    every call return None.
    """
    found = _find_job_info_by_key(jobs_data)
    if found:
        return found
    return _find_job_info_by_shape(jobs_data)


def _find_job_info_by_key(jobs_data) -> list | None:
    if isinstance(jobs_data, dict):
        for key, value in jobs_data.items():
            if key in _KNOWN_JOB_KEYS and isinstance(value, list):
                return value
            result = _find_job_info_by_key(value)
            if result:
                return result
    elif isinstance(jobs_data, list):
        for item in jobs_data:
            result = _find_job_info_by_key(item)
            if result:
                return result
    return None


def _find_job_info_by_shape(jobs_data) -> list | None:
    """Last-resort fallback: walk the JSON looking for arrays that
    structurally match a job record. Returns the wrapping array when
    we find one (matching the known-key path's return shape).
    """
    if isinstance(jobs_data, list):
        if jobs_data and _looks_like_job_record(jobs_data[0]):
            return jobs_data
        for item in jobs_data:
            result = _find_job_info_by_shape(item)
            if result:
                return result
    elif isinstance(jobs_data, dict):
        for value in jobs_data.values():
            result = _find_job_info_by_shape(value)
            if result:
                return result
    return None


def find_job_info_initial_page(html_text: str):
    """Extract the first batch of job records from the search HTML.

    Tries each known wrapper key in turn. If none match (Google
    rotated the ID), falls back to scanning all 9-digit JSON keys
    that wrap a list-of-lists payload — the structural shape Google
    uses for the jobs panel doesn't change even when the wrapping
    function ID does.
    """
    results: list = []

    for key in _KNOWN_JOB_KEYS:
        pattern = key + r'":(\[.*?\]\s*])\s*}\s*]\s*]\s*]\s*]\s*]'
        matches = list(re.finditer(pattern, html_text))
        if not matches:
            continue
        for match in matches:
            try:
                results.append(json.loads(match.group(1)))
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse match for key {key}: {e}")
        if results:
            return results

    # Fallback: probe every 9-digit key that immediately precedes a
    # list-of-lists. We log unfamiliar keys so the maintainer can
    # bake the working one back into `_KNOWN_JOB_KEYS`.
    fallback_pattern = r'"(\d{9})":(\[\[\[.*?\]\s*])\s*}\s*]\s*]\s*]\s*]\s*]'
    for match in re.finditer(fallback_pattern, html_text):
        key = match.group(1)
        try:
            parsed = json.loads(match.group(2))
        except json.JSONDecodeError:
            continue
        if not parsed or not _looks_like_job_record(parsed[0]):
            continue
        log.warning(
            "Google: matched jobs payload via fallback key %s — "
            "consider adding it to _KNOWN_JOB_KEYS",
            key,
        )
        results.append(parsed)
    return results
