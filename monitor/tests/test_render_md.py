"""Tests for monitor/render_md.py — entry-level filter + classifier.

Targets the bug where ``"intern" in title`` matched ``Internal`` and
sent Senior / Lead / SRE roles into the intern bucket of the prior
emea-entry-level view, plus the tech-shape + batch-hire title gate
that mirrors speedyapply's NEW_GRAD_*.md shape and the
emea-graduate.md / na-graduate.md graduate-only renderers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from monitor import render_md


def _row(
    title: str,
    *,
    site: str = "indeed",
    region: str = "emea",
    company: str = "Acme",
    location: str = "London, UK",
    first_seen: str = "2026-05-15T12:00:00+00:00",
) -> dict:
    return {
        "title": title,
        "site": site,
        "region": region,
        "company": company,
        "location": location,
        "company_url": "",
        "job_url": "https://example.test/jobs/1",
        "description": "x" * 800,
        "date_posted": first_seen,
        "first_seen": first_seen,
        "last_seen": first_seen,
        "liveness_status": "ok",
        "signature": f"sig::{title.lower()}::{company.lower()}",
    }


# --------------------------------------------------------------------------- #
# _classify_intern_or_newgrad — the "Internal" substring bug
# --------------------------------------------------------------------------- #


class TestClassifyInternOrNewgrad:
    @pytest.mark.parametrize(
        "title",
        [
            "Senior Full Stack Engineer (Copilot Agents & Internal Productivity Products)",
            "Site Reliability Engineer (f/m/d) – Observability & Internal Tools",
            "Lead Engineer – Internal Platforms & AI",
            "Software Engineer, Internal Infrastructure (Europe & UK)",
            "International Customer Engineer",
        ],
    )
    def test_internal_does_not_match_intern(self, title):
        """Titles containing 'Internal' / 'International' must NOT classify as intern."""
        assert render_md._classify_intern_or_newgrad(_row(title)) == "newgrad"

    @pytest.mark.parametrize(
        "title",
        [
            "Software Engineer Intern",
            "Machine Learning Intern",
            "Software Engineering Internship - Summer 2026",
            "Intern, Backend Engineering",
            "Praktikum: Data Engineer",
            "Stagiaire Data Analyst",
            "Stage : Software development engineer",
            "Industrial Placement Software Engineer",
            "Year in Industry Software Engineer",
            "Software Engineer Trainee",
        ],
    )
    def test_real_intern_titles_classify_as_intern(self, title):
        assert render_md._classify_intern_or_newgrad(_row(title)) == "intern"

    def test_site_label_intern_short_circuits(self):
        """A SimplifyJobs intern-source row classifies via site, not title."""
        r = _row("Software Engineer, New Grad", site="simplify_intern")
        assert render_md._classify_intern_or_newgrad(r) == "intern"

    def test_site_label_newgrad_short_circuits(self):
        r = _row("Software Engineer Intern", site="simplify_newgrad")
        assert render_md._classify_intern_or_newgrad(r) == "newgrad"

    def test_stage_word_boundary(self):
        """`stage` matches as standalone but not as a sub-word."""
        assert (
            render_md._classify_intern_or_newgrad(_row("Backstage Platform Engineer"))
            == "newgrad"
        )
        assert (
            render_md._classify_intern_or_newgrad(_row("Multi-stage Pipeline Engineer"))
            == "newgrad"
        )
        assert (
            render_md._classify_intern_or_newgrad(_row("Stage : Software developer"))
            == "intern"
        )


# --------------------------------------------------------------------------- #
# _title_has_tech_shape — drops non-software role families
# --------------------------------------------------------------------------- #


class TestTechShape:
    @pytest.mark.parametrize(
        "title",
        [
            "software engineer",
            "junior software engineer",
            "graduate software engineer",
            "software developer",
            "backend engineer",
            "fullstack engineer",
            "machine learning engineer",
            "data scientist",
            "data analyst",
            "applied scientist",
            "ml engineer",
            "ai engineer",
            "research engineer, foundations",
            "member of technical staff",
            "site reliability engineer",
            "platform engineer",
            "embedded software engineer",
            "qa engineer",
            "ios developer",
            "algorithm engineer",
            "computer vision engineer",
        ],
    )
    def test_tech_shape_titles(self, title):
        assert render_md._title_has_tech_shape(title)

    @pytest.mark.parametrize(
        "title",
        [
            "vehicle testing engineer internship, powertrain engineering europe",
            "battery testing engineer internship",
            "technical program management internship",
            "graduate project engineer",
            "process engineering intern - process engineering / chemical engineering",
            "product engineer intern",
            "planning engineer - internship",
            "maintenance engineer (electrical bias)",
            "graduate mechatronics engineer",
        ],
    )
    def test_non_tech_shape_titles(self, title):
        assert not render_md._title_has_tech_shape(title)

    def test_lecturer_filtered_by_seniority_drop(self):
        """`Lecturer in Software Engineering` passes tech-shape via the
        `software engineer` substring but is caught by the academic-role
        drop list."""
        r = _row("Lecturer in Software Engineering and Information Technology")
        assert render_md._title_has_tech_shape(r["title"].lower())
        assert not render_md._title_passes_entry_level_filter(r, "newgrad")


# --------------------------------------------------------------------------- #
# _title_has_batch_hire_marker — graduate-programme intake detection
# --------------------------------------------------------------------------- #


class TestBatchHireMarker:
    @pytest.mark.parametrize(
        "title",
        [
            "software engineer, new grad",
            "new grad software engineer",
            "graduate software engineer",
            "software engineer graduate",
            "early career software engineer",
            "campus hire software engineer",
            "university hire - swe",
            "university graduate software engineer",
            "associate software engineer",
            "junior software engineer",
            "entry level software engineer",
            "class of 2026 software engineer",
            "software engineer i",
            "software engineer 1",
            "sde i, ec2",
            "rotational software engineer",
            "apprentice software engineer",
            "trainee software engineer",
        ],
    )
    def test_batch_hire_markers(self, title):
        assert render_md._title_has_batch_hire_marker(title)

    @pytest.mark.parametrize(
        "title",
        [
            "software engineer",
            "software engineer, safeguards foundations (internal tooling)",
            "machine learning engineer, search",
            "data scientist - growth",
            "backend engineer, payments",
        ],
    )
    def test_specific_role_no_marker(self, title):
        assert not render_md._title_has_batch_hire_marker(title)


# --------------------------------------------------------------------------- #
# _title_passes_entry_level_filter — full gate, source-aware
# --------------------------------------------------------------------------- #


class TestEntryLevelFilter:
    def test_drops_senior_even_with_intern_marker(self):
        # Real case from the prior emea-entry-level view: bug-routed
        # senior into intern.
        r = _row(
            "Senior Full Stack Engineer (Copilot Agents & Internal Productivity Products)"
        )
        assert not render_md._title_passes_entry_level_filter(r, "intern")
        assert not render_md._title_passes_entry_level_filter(r, "newgrad")

    def test_drops_lead_engineer(self):
        r = _row("Lead Engineer – Internal Platforms & AI")
        assert not render_md._title_passes_entry_level_filter(r, "newgrad")

    def test_drops_non_tech_internship(self):
        r = _row("Vehicle Testing Engineer Internship, Powertrain Engineering")
        assert not render_md._title_passes_entry_level_filter(r, "intern")

    def test_keeps_software_intern(self):
        r = _row("Software Engineer Intern")
        assert render_md._title_passes_entry_level_filter(r, "intern")

    def test_newgrad_jobspy_requires_batch_marker(self):
        # Indeed (JobSpy) source — bare 'Software Engineer' without batch
        # marker should be dropped from the new-grad section.
        r = _row("Software Engineer", site="indeed")
        assert not render_md._title_passes_entry_level_filter(r, "newgrad")

    def test_newgrad_jobspy_passes_with_batch_marker(self):
        r = _row("Graduate Software Engineer", site="indeed")
        assert render_md._title_passes_entry_level_filter(r, "newgrad")

    def test_newgrad_simplify_passes_without_batch_marker(self):
        """Curated upstream (SimplifyJobs) bypasses the batch-marker gate —
        same coverage guarantee as speedyapply."""
        r = _row("Software Engineer", site="simplify_newgrad")
        assert render_md._title_passes_entry_level_filter(r, "newgrad")

    def test_newgrad_direct_ats_still_needs_batch_marker(self):
        """`direct:*` ATS feeds pull every open role at the company without
        seniority curation, so the batch-marker gate applies (unlike
        SimplifyJobs which is hand-curated to the new-grad audience)."""
        r = _row("Software Engineer, Safeguards Foundations", site="direct:anthropic")
        assert not render_md._title_passes_entry_level_filter(r, "newgrad")
        r2 = _row("Anthropic Fellows Program — ML Systems", site="direct:anthropic")
        assert render_md._title_passes_entry_level_filter(r2, "newgrad")

    def test_intern_non_tech_drops_even_from_simplify(self):
        """Tech-shape gate applies to ALL sources — speedyapply's intern
        upstream sometimes includes mech/chem roles we want to drop."""
        r = _row(
            "Process Engineering Intern - Chemical Engineering",
            site="simplify_intern",
        )
        assert not render_md._title_passes_entry_level_filter(r, "intern")


# --------------------------------------------------------------------------- #
# render_region_graduate — end-to-end smoke test
# --------------------------------------------------------------------------- #


class TestRenderRegionGraduate:
    def test_emea_renders_and_filters(self, tmp_path):
        rows = [
            _row("Graduate Software Engineer", region="emea", company="UK Co"),
            _row("Software Engineer Intern", region="emea", company="Intern Co"),
            _row("Vehicle Testing Engineer Internship", region="emea", company="Tesla"),
            _row(
                "Senior Full Stack Engineer (Internal Productivity)",
                region="emea",
                company="X",
            ),
            _row(
                "Software Engineer, New Grad",
                region="north_america",
                site="simplify_newgrad",
                company="US Co",
            ),
        ]
        path = tmp_path / "emea-graduate.md"
        n = render_md.render_region_graduate(rows, "emea", path)
        body = path.read_text(encoding="utf-8")

        # Header is region-specific.
        assert "EMEA Graduate Roles" in body
        # NA-tagged row never reaches EMEA.
        assert "US Co" not in body
        # Non-tech and senior rows are filtered.
        assert "Vehicle Testing Engineer" not in body
        assert "Senior Full Stack Engineer" not in body
        # Intern row is dropped entirely from the graduate view.
        assert "Software Engineer Intern" not in body
        assert "Intern Co" not in body
        # Real graduate role renders.
        assert "Graduate Software Engineer" in body
        # Sanity: only one row passed for EMEA (the new-grad title).
        assert n == 1

    def test_na_renders_separately(self, tmp_path):
        rows = [
            _row(
                "Software Engineer, New Grad",
                region="north_america",
                site="simplify_newgrad",
                company="Stripe US",
            ),
            _row(
                "Software Engineering Intern - Summer 2026",
                region="north_america",
                site="simplify_intern",
                company="Snowflake",
            ),
            _row("Graduate Software Engineer", region="emea", company="EU Co"),
        ]
        path = tmp_path / "na-graduate.md"
        n = render_md.render_na_graduate(rows, path)
        body = path.read_text(encoding="utf-8")

        assert "North America Graduate Roles" in body
        assert "Stripe US" in body
        # Intern row is dropped from the graduate view.
        assert "Snowflake" not in body
        # EMEA row doesn't leak into NA.
        assert "EU Co" not in body
        assert n == 1

    def test_emea_renderer(self, tmp_path):
        rows = [_row("Graduate Software Engineer", region="emea")]
        path = tmp_path / "emea-graduate.md"
        n = render_md.render_emea_graduate(rows, path)
        assert n == 1
        assert "EMEA Graduate Roles" in path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Remote-jobs slice — _row_is_remote + the remote_only slice filter
# --------------------------------------------------------------------------- #


class TestRowIsRemote:
    @pytest.mark.parametrize(
        "title, location",
        [
            ("Software Engineer", "Remote · USA"),
            ("Software Engineer", "Remote in Canada · Toronto, ON"),
            ("AI Engineer - US Remote", "New York, NY"),
            ("Senior Backend Engineer", "Anywhere"),
            ("Platform Engineer", "Work from home"),
            ("Backend Engineer", "WFH · Berlin"),
            ("ML Engineer", "Fully Remote, EU"),
            ("SWE", "Remote-first · London"),
        ],
    )
    def test_remote_markers_match(self, title, location):
        assert render_md._row_is_remote(_row(title, location=location))

    @pytest.mark.parametrize(
        "title, location",
        [
            ("Software Engineer", "London, UK"),
            ("Internal Platform Engineer", "Berlin, Germany"),
            ("Promoter Engineer", "Madrid, Spain"),
            ("International Customer Engineer", "Cheltenham, ENG, GB"),
            ("Software Engineer", "Cambridge, MA"),
        ],
    )
    def test_non_remote_does_not_match(self, title, location):
        """Substring traps: 'Internal', 'Promoter', 'International'
        all contain the literal letters 'remote'/'wfh' adjacent to other
        letters or as part of a larger token — word-boundary regex must
        not match them.
        """
        assert not render_md._row_is_remote(
            _row(title, location=location)
        )


class TestRemoteOnlyFilter:
    def test_remote_only_keeps_only_remote_rows(self):
        sfilters = {"remote_only": True}
        remote_row = _row("Software Engineer", location="Remote · USA")
        onsite_row = _row("Software Engineer", location="London, UK")
        assert render_md._matches_slice_filters(remote_row, sfilters)
        assert not render_md._matches_slice_filters(onsite_row, sfilters)

    def test_remote_only_combines_with_other_filters(self):
        """remote_only is additive — other filters still apply on top."""
        sfilters = {
            "remote_only": True,
            "title_keywords_none": ["staff"],
        }
        remote_jr = _row("Junior Software Engineer", location="Remote · USA")
        remote_staff = _row("Staff Software Engineer", location="Remote · USA")
        assert render_md._matches_slice_filters(remote_jr, sfilters)
        assert not render_md._matches_slice_filters(remote_staff, sfilters)

    def test_remote_slice_renders_cross_region(self, tmp_path):
        rows = [
            _row(
                "Software Engineer",
                region="emea",
                company="EU Startup",
                location="Remote · EU",
            ),
            _row(
                "Backend Engineer",
                region="north_america",
                company="US Startup",
                location="Remote · USA",
            ),
            _row(
                "Software Engineer",
                region="emea",
                company="OnsiteCo",
                location="London, UK",
            ),
        ]
        slice_def = {
            "name": "remote_jobs",
            "title": "Remote Jobs (Startups & More)",
            "filters": {"remote_only": True},
        }
        path = tmp_path / "remote-jobs.md"
        stats = render_md.render_slice(rows, slice_def, path)
        body = path.read_text(encoding="utf-8")

        assert stats["total"] == 2
        assert "EU Startup" in body
        assert "US Startup" in body
        assert "OnsiteCo" not in body
