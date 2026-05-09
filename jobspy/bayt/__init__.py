from __future__ import annotations

import random
import re
import time
import unicodedata

from bs4 import BeautifulSoup

from jobspy.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    JobResponse,
    Location,
    Country,
)
from jobspy.util import create_logger, create_session

log = create_logger("Bayt")

# Bayt 403's the default `python-requests/X.X` UA on the first request.
# Setting a real Chrome UA (and matching Accept-Language) gets us past
# the WAF for the search-results pages.
_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/138.0.0.0 Safari/537.36"
)


def _slugify_query(query: str) -> str:
    """Bayt URL slugs are hyphenated lowercase (e.g. `software-engineer-jobs`).

    Spaces in the path produce a 404 with no listings, which previously
    showed up as a SILENT source. Lowercase, fold accented Latin chars
    to ASCII (so "ingénieur logiciel" → "ingenieur-logiciel" rather
    than "ingnieur-logiciel"), collapse whitespace to `-`, drop
    characters Bayt's slug grammar doesn't accept.
    """
    q = (query or "").strip().lower()
    q = unicodedata.normalize("NFKD", q).encode("ascii", "ignore").decode("ascii")
    q = re.sub(r"\s+", "-", q)
    q = re.sub(r"[^a-z0-9\-]", "", q)
    q = re.sub(r"-+", "-", q).strip("-")
    return q


class BaytScraper(Scraper):
    base_url = "https://www.bayt.com"
    delay = 2
    band_delay = 3

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        super().__init__(Site.BAYT, proxies=proxies, ca_cert=ca_cert, user_agent=user_agent)
        self.scraper_input = None
        self.session = None
        self.country = "worldwide"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        self.scraper_input = scraper_input
        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, is_tls=False, has_retry=True
        )
        # Without a browser-like UA Bayt's WAF returns 403 to every
        # request, which previously made every call silently return 0
        # rows (classified as SILENT). Set a real Chrome UA on the
        # session so the WAF lets the search pages through.
        self.session.headers.update({
            "User-Agent": self.user_agent or _DEFAULT_UA,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })
        job_list: list[JobPost] = []
        page = 1
        results_wanted = (
            scraper_input.results_wanted if scraper_input.results_wanted else 10
        )

        while len(job_list) < results_wanted:
            log.info(f"Fetching Bayt jobs page {page}")
            job_elements = self._fetch_jobs(self.scraper_input.search_term, page)
            if not job_elements:
                break

            if job_elements:
                log.debug(
                    "First job element snippet:\n" + job_elements[0].prettify()[:500]
                )

            initial_count = len(job_list)
            for job in job_elements:
                try:
                    job_post = self._extract_job_info(job)
                    if job_post:
                        job_list.append(job_post)
                        if len(job_list) >= results_wanted:
                            break
                    else:
                        log.debug(
                            "Extraction returned None. Job snippet:\n"
                            + job.prettify()[:500]
                        )
                except Exception as e:
                    log.error(f"Bayt: Error extracting job info: {str(e)}")
                    continue

            if len(job_list) == initial_count:
                log.info(f"No new jobs found on page {page}. Ending pagination.")
                break

            page += 1
            time.sleep(random.uniform(self.delay, self.delay + self.band_delay))

        job_list = job_list[: scraper_input.results_wanted]
        return JobResponse(jobs=job_list)

    def _fetch_jobs(self, query: str, page: int) -> list | None:
        """
        Grabs the job results for the given query and page number.

        Errors are RAISED, not swallowed. Previously a 403/404 returned
        None and the caller broke out of pagination, producing 0 rows
        with no exception — which the monitor's health tracker
        misclassifies as SILENT (IP block) when in fact every call
        threw. Re-raising lets the monitor record an error and report
        the source as BROKEN.
        """
        slug = _slugify_query(query)
        if not slug:
            raise ValueError(
                f"Bayt: empty query slug after sanitization (query={query!r})"
            )
        url = f"{self.base_url}/en/international/jobs/{slug}-jobs/?page={page}"
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        job_listings = soup.find_all("li", attrs={"data-js-job": ""})
        log.debug(f"Found {len(job_listings)} job listing elements")
        return job_listings

    def _extract_job_info(self, job: BeautifulSoup) -> JobPost | None:
        """
        Extracts the job information from a single job listing.
        """
        # Find the h2 element holding the title and link (no class filtering)
        job_general_information = job.find("h2")
        if not job_general_information:
            return

        job_title = job_general_information.get_text(strip=True)
        job_url = self._extract_job_url(job_general_information)
        if not job_url:
            return

        # Extract company name using the original approach:
        company_tag = job.find("div", class_="t-nowrap p10l")
        company_name = (
            company_tag.find("span").get_text(strip=True)
            if company_tag and company_tag.find("span")
            else None
        )

        # Extract location using the original approach:
        location_tag = job.find("div", class_="t-mute t-small")
        location = location_tag.get_text(strip=True) if location_tag else None

        job_id = f"bayt-{abs(hash(job_url))}"
        location_obj = Location(
            city=location,
            country=Country.from_string(self.country),
        )
        return JobPost(
            id=job_id,
            title=job_title,
            company_name=company_name,
            location=location_obj,
            job_url=job_url,
        )

    def _extract_job_url(self, job_general_information: BeautifulSoup) -> str | None:
        """
        Pulls the job URL from the 'a' within the h2 element.
        """
        a_tag = job_general_information.find("a")
        if a_tag and a_tag.has_attr("href"):
            return self.base_url + a_tag["href"].strip()
