"""
Script to generate a detailed CSV of issue-level data for Plans datasets from
the Datasette service. It joins issue summaries with provision data to create
a merged view of expected dataset performance per organisation.
"""

import os
import pandas as pd
import argparse
from utils import get_http_session

# Plans Pipelines
ALL_PIPELINES = [
    "local-plan",
    "minerals-plan",
    "plan-timetable",
    "supplementary-plan",
    "waste-plan",
]

# Datasette Query Helper
def get_datasette_query(db: str, sql: str, url="https://datasette.planning.data.gov.uk") -> pd.DataFrame:
    full_url = f"{url}/{db}.json"
    params = {"sql": sql, "_shape": "array", "_size": "max"}
    http = get_http_session()
    response = http.get(full_url, params=params)
    response.raise_for_status()
    return pd.DataFrame(response.json())

# Provision Query
def get_provisions():
    sql = """
        SELECT
            p.organisation,
            o.name AS organisation_name
        FROM provision p
        INNER JOIN organisation o ON o.organisation = p.organisation
        WHERE p.specification = "local-plan"
          AND p.provision_reason = "statutory"
        GROUP BY p.organisation
    """
    return get_datasette_query("digital-land", sql)

# Issue Query (Paged)
def get_issue_type_chunk(dataset_clause, offset):
    sql = f"""
        SELECT
            edits.*,
            eds.endpoint_end_date,
            eds.endpoint_entry_date,
            eds.latest_status,
            eds.latest_exception
        FROM endpoint_dataset_issue_type_summary edits
        LEFT JOIN (
            SELECT endpoint, dataset, end_date as endpoint_end_date,
                   entry_date as endpoint_entry_date,
                   latest_status, latest_exception
            FROM endpoint_dataset_summary
        ) eds ON edits.endpoint = eds.endpoint AND edits.dataset = eds.dataset
        {dataset_clause}
        LIMIT 1000 OFFSET {offset}
    """
    return get_datasette_query("performance", sql)

def get_full_issue_type_summary(pipelines):
    dataset_clause = "WHERE " + " OR ".join(f"edits.dataset = '{p}'" for p in pipelines)
    df_list = []
    offset = 0
    while True:
        chunk = get_issue_type_chunk(dataset_clause, offset)
        if chunk.empty:
            break
        df_list.append(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    return pd.concat(df_list, ignore_index=True)

# Main CSV Generator
def generate_plans_issues_csv(output_dir: str) -> str:
    print("[INFO] Fetching provisions...")
    provisions = get_provisions()

    print("[INFO] Fetching detailed issue-level data...")
    issues = get_full_issue_type_summary(ALL_PIPELINES)

    # Normalise organisation codes (remove -eng suffix)
    issues["organisation"] = issues["organisation"].str.replace("-eng", "", regex=False)

    print("[INFO] Merging data...")
    merged = provisions.merge(
        issues.drop(columns=["organisation_name"], errors="ignore"),
        on="organisation",
        how="inner"
    )

    print("[INFO] Saving CSV...")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "plan_issue.csv")
    merged[
        [
            "organisation",
            "organisation_name",
            "pipeline",
            "issue_type",
            "field",
            "severity",
            "responsibility",
            "count_issues",
            "collection",
            "endpoint",
            "endpoint_url",
            "latest_status",
            "latest_exception",
            "resource",
            "latest_log_entry_date",
            "endpoint_entry_date",
            "endpoint_end_date",
            "resource_start_date",
            "resource_end_date",
        ]
    ].to_csv(output_path, index=False)

    print(f"[SUCCESS] CSV saved: {output_path} ({len(merged)} rows)")
    return output_path

# CLI Argument Parser
def parse_args():
    parser = argparse.ArgumentParser(description="Generate detailed Plans issue-level CSV")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save the output CSV"
    )
    return parser.parse_args()

# Script Entry Point
if __name__ == "__main__":
    args = parse_args()
    generate_plans_issues_csv(args.output_dir)
