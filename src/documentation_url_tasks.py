import argparse
import json
import logging
import os

import pandas as pd

from utils import read_csv_with_retry, task_url

logger = logging.getLogger(__name__)

FILES_URL = os.environ.get("FILES_URL", "https://files.planning.data.gov.uk")
ORGS_URL = f"{FILES_URL}/organisation-collection/dataset/organisation.csv"

# The task pipeline (pyspark-jobs) checks each live endpoint is still linked from its
# documentation page and raises a task with this task_source when it is not. The task
# table is the source of truth: this report only flattens those tasks for reading.
TASK_SOURCE = "documentation-url"

# Most actionable first: the page no longer links to the data, then the page itself
# has gone, then the page could not be read so nothing is known.
RESULT_ORDER = ["not-found", "documentation-url-gone", "unreachable"]

OUTPUT_COLUMNS = [
    "organisation",
    "organisation_name",
    "dataset",
    "endpoint",
    "endpoint_url",
    "documentation_urls",
    "result",
    "severity",
    "likely_replacement",
    "redirects",
    "guidance",
    "fetch_errors",
    "candidate_links",
    "candidate_link_count",
    "entry_date",
    "reference",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Report endpoints that are not linked from their documentation URL."
    )
    parser.add_argument("--output-dir", type=str, required=True, help="Directory to save the output CSV")
    return parser.parse_args()


def guidance(row) -> str:
    """What the LPA needs to do, worded so it can be forwarded to them."""
    data = f"{row['dataset'].replace('-', ' ')} data" if row["dataset"] else "data"
    pages = row["documentation_urls"]

    if row["result"] == "not-found":
        if row["likely_replacement"]:
            return (
                f"We collect your {data} from {row['endpoint_url']}, but this is no longer linked from "
                f"{pages}. That page now links to {row['likely_replacement']}, which looks like a newer "
                "version. If it is, give us the new URL so we collect your latest data."
            )
        if row["redirects"]:
            return (
                f"The page {row['redirects']}, and no longer links to {row['endpoint_url']}. If your "
                f"{data} has moved, give us the URL of the page where you now publish it, and the URL "
                "of the data file."
            )
        return (
            f"We collect your {data} from {row['endpoint_url']}, but this is no longer linked from "
            f"{pages}. If you've published a new version, give us its URL. If the file is still "
            "current, add a link to it on that page so people can find it."
        )
    if row["result"] == "documentation-url-gone":
        return (
            f"The page {pages} no longer exists. Give us the URL of the page where you now publish "
            f"your {data}."
        )
    return (
        f"We couldn't open {pages} to check that it links to your {data}. Check that the page is "
        "public and can be opened without signing in."
    )


def _joined(values) -> str:
    # a key missing from some tasks' details comes through as NaN, not a list
    return "; ".join(values) if isinstance(values, list) else ""


def _redirects(values) -> str:
    if not isinstance(values, dict):
        return ""
    return "; ".join(f"{old} now goes to {new}" for old, new in values.items())


def build_report(tasks: pd.DataFrame, orgs: pd.DataFrame) -> pd.DataFrame:
    """
    One row per documentation URL task, with its details spread into columns.

    Lists (documentation URLs, fetch errors, candidate links) are joined with "; "
    so the CSV stays one row per task, and each row gets guidance for the LPA.
    """
    tasks = tasks[tasks["task_source"] == TASK_SOURCE].copy()
    if tasks.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # json_normalize would flatten redirects into a column per URL, so the details
    # are taken key by key instead
    details = pd.DataFrame(tasks["details"].map(json.loads).tolist(), index=tasks.index)
    for column in ["documentation_urls", "fetch_errors", "candidate_links"]:
        details[column] = details.get(column, pd.Series(index=details.index, dtype=object)).map(_joined)
    details["redirects"] = details.get("redirects", pd.Series(index=details.index, dtype=object)).map(_redirects)
    details["likely_replacement"] = details.get("likely_replacement", pd.Series(index=details.index, dtype=object)).fillna("")
    details["candidate_link_count"] = details["candidate_links"].map(
        lambda links: len(links.split("; ")) if links else 0
    )

    report = tasks.drop(columns=["details"]).join(details.drop(columns=["task_type"], errors="ignore"))
    report = report.merge(
        orgs[["organisation", "name"]].rename(columns={"name": "organisation_name"}),
        on="organisation",
        how="left",
    )
    report["guidance"] = report.apply(guidance, axis=1)
    report["_order"] = report["result"].map({result: i for i, result in enumerate(RESULT_ORDER)}).fillna(len(RESULT_ORDER))
    report = report.sort_values(["_order", "organisation", "dataset", "endpoint"])
    return report[OUTPUT_COLUMNS].reset_index(drop=True)


def main(output_dir):
    tasks = read_csv_with_retry(task_url(), dtype=str, keep_default_na=False)
    orgs = read_csv_with_retry(ORGS_URL, dtype=str, keep_default_na=False)

    report = build_report(tasks, orgs)

    counts = report["result"].value_counts().to_dict() if not report.empty else {}
    print(f"Documentation URL tasks: {len(report)} {counts}")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "documentation_url_tasks.csv")
    report.to_csv(output_path, index=False)
    print(f"CSV saved: {output_path}")


if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)
