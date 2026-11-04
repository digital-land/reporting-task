import json

import pandas as pd

from documentation_url_tasks import OUTPUT_COLUMNS, build_report

TASK_COLUMNS = ["dataset", "organisation", "endpoint", "resource", "details", "severity",
                "responsibility", "task_source", "entry_date", "quality_dimension", "reference"]

ORGS = pd.DataFrame([{"organisation": "local-authority:BRO", "name": "Bromley"}])

PAGE = "https://council.gov.uk/a"
OLD = "https://council.gov.uk/old.csv"
NEW = "https://council.gov.uk/2026/old.csv"


def task(task_source="documentation-url", result="not-found", endpoint="e1", **details):
    """A task row as the task pipeline writes it. An unreachable or gone task has
    no redirects, candidate links or likely replacement in its details."""
    body = {
        "task_type": "endpoint_url_in_documentation_url",
        "result": result,
        "endpoint_url": OLD,
        "documentation_urls": [PAGE],
        "fetch_errors": [],
    }
    if result == "not-found":
        body.update({"redirects": {}, "candidate_links": [], "likely_replacement": ""})
    body.update(details)
    severity = "notice" if result == "unreachable" else "warning"
    return ["plan-timetable", "local-authority:BRO", endpoint, "", json.dumps(body), severity,
            "external", task_source, "2026-11-04", "integrity", f"ref-{endpoint}"]


def report_of(*rows):
    return build_report(pd.DataFrame(list(rows), columns=TASK_COLUMNS), ORGS)


def test_only_documentation_url_tasks_are_reported():
    report = report_of(task(), task(task_source="issue", endpoint="e2"))

    assert report["endpoint"].tolist() == ["e1"]
    assert report.columns.tolist() == OUTPUT_COLUMNS


def test_details_become_columns_with_organisation_name():
    report = report_of(task(
        documentation_urls=[PAGE, "https://council.gov.uk/b"],
        fetch_errors=["http-403"],
        candidate_links=[NEW, "https://council.gov.uk/other.csv"],
        likely_replacement=NEW,
    ))
    row = report.iloc[0]

    assert row["organisation_name"] == "Bromley"
    assert row["endpoint_url"] == OLD
    assert row["documentation_urls"] == f"{PAGE}; https://council.gov.uk/b"
    assert row["fetch_errors"] == "http-403"
    assert row["candidate_links"] == f"{NEW}; https://council.gov.uk/other.csv"
    assert row["candidate_link_count"] == 2
    assert row["likely_replacement"] == NEW


def test_redirects_are_one_readable_column():
    report = report_of(task(redirects={PAGE: "https://council.gov.uk/heritage"}))

    assert report.iloc[0]["redirects"] == f"{PAGE} now goes to https://council.gov.uk/heritage"


def test_results_sort_by_how_actionable_they_are():
    report = report_of(
        task(result="unreachable", endpoint="e1"),
        task(result="documentation-url-gone", endpoint="e2"),
        task(result="not-found", endpoint="e3"),
    )

    assert report["result"].tolist() == ["not-found", "documentation-url-gone", "unreachable"]
    unreachable = report.iloc[2]
    assert unreachable["candidate_links"] == ""
    assert unreachable["candidate_link_count"] == 0
    assert unreachable["likely_replacement"] == ""
    assert unreachable["redirects"] == ""


def test_guidance_names_the_likely_replacement():
    guidance = report_of(task(likely_replacement=NEW)).iloc[0]["guidance"]

    assert guidance.startswith(f"We collect your plan timetable data from {OLD}")
    assert f"That page now links to {NEW}" in guidance


def test_guidance_for_a_page_that_moved():
    guidance = report_of(task(redirects={PAGE: "https://council.gov.uk/heritage"})).iloc[0]["guidance"]

    assert guidance.startswith(f"The page {PAGE} now goes to https://council.gov.uk/heritage")


def test_guidance_when_nothing_on_the_page_explains_it():
    guidance = report_of(task()).iloc[0]["guidance"]

    assert "If you've published a new version, give us its URL." in guidance


def test_guidance_for_a_page_that_is_gone_or_unreadable():
    report = report_of(
        task(result="documentation-url-gone", endpoint="e1"),
        task(result="unreachable", endpoint="e2"),
    )

    assert report.iloc[0]["guidance"].startswith(f"The page {PAGE} no longer exists.")
    assert report.iloc[1]["guidance"].startswith(f"We couldn't open {PAGE}")


def test_no_documentation_url_tasks_gives_an_empty_report():
    report = report_of(task(task_source="issue"))

    assert report.empty
    assert report.columns.tolist() == OUTPUT_COLUMNS
