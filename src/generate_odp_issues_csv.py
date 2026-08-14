"""
Script to generate a detailed CSV of issue-level data from the Open Digital Planning (ODP)
Datasette service. It joins issue summaries with provision data to create a merged view 
of expected dataset performance per organisation and cohort.
"""

import os
import pandas as pd
import argparse
from utils import datasette_query, datasette_query_paginated

# Dataset Definitions
SPATIAL_DATASETS = [
    "article-4-direction-area",
    "conservation-area",
    "listed-building-outline",
    "tree-preservation-zone",
    "tree",
]
DOCUMENT_DATASETS = [
    "article-4-direction",
    "conservation-area-document",
    "tree-preservation-order",
]
ALL_DATASETS = SPATIAL_DATASETS + DOCUMENT_DATASETS

# Provision Query
def get_provisions():
    """
    Retrieves all expected dataset provisions from the 'provision' table.

    Returns:
        pd.DataFrame: Provision records joined with cohort and organisation names.
    """
    sql = """
        SELECT
            p.cohort,
            p.organisation,
            c.start_date AS cohort_start_date,
            o.name AS organisation_name
        FROM provision p
        INNER JOIN cohort c ON c.cohort = p.cohort
        INNER JOIN organisation o ON o.organisation = p.organisation
        WHERE p.provision_reason = 'expected'
          AND p.project = 'open-digital-planning'
        GROUP BY p.organisation, p.cohort
    """
    return datasette_query("digital-land", sql)

# Issue Query (Paged)
def get_full_issue_type_summary(datasets):
    """
    Retrieves the full issue summary table across all datasets using pagination.

    Args:
        datasets (list): List of dataset names to include.

    Returns:
        pd.DataFrame: Combined issue summary for all specified datasets.
    """
    dataset_clause = "WHERE " + " OR ".join(f"edits.dataset = '{ds}'" for ds in datasets)
    sql = f"""
        SELECT
            edits.*,
            eds.endpoint_end_date,
            eds.endpoint_entry_date,
            eds.latest_status,
            eds.latest_exception
        FROM endpoint_dataset_issue_type_summary edits
        LEFT JOIN (
            SELECT endpoint, end_date as endpoint_end_date,
                   entry_date as endpoint_entry_date,
                   latest_status, latest_exception
            FROM endpoint_dataset_summary
        ) eds ON edits.endpoint = eds.endpoint
        {dataset_clause}
    """
    return datasette_query_paginated("performance", sql, page_size=1000)

# Main CSV Generator
def generate_detailed_issue_csv(output_dir: str, dataset_type="all") -> str:
    """
    Generates a CSV containing detailed issue-level data for ODP datasets.

    Args:
        output_dir (str): Path to the output directory.
        dataset_type (str): One of 'spatial', 'document', or 'all' (default).

    Returns:
        str: Path to the saved CSV file.
    """
    # Select datasets based on type
    datasets = {
        "spatial": SPATIAL_DATASETS,
        "document": DOCUMENT_DATASETS,
        "all": ALL_DATASETS
    }.get(dataset_type, ALL_DATASETS)

    print("[INFO] Fetching provisions...")
    provisions = get_provisions()

    print("[INFO] Fetching detailed issue-level data...")
    issues = get_full_issue_type_summary(datasets)

    print("[INFO] Merging data...")
    merged = provisions.merge(
        issues.drop(columns=["organisation_name"], errors="ignore"),
        on=["organisation", "cohort"],
        how="inner"
    )

    print("[INFO] Saving CSV...")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "odp_issue.csv")
    merged[
        [
            "organisation",
            "cohort",
            "organisation_name",
            "pipeline",
            "issue_type",
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
    """
    Parses command-line arguments for the script.

    Returns:
        argparse.Namespace: Contains the '--output-dir' argument.
    """
    parser = argparse.ArgumentParser(description="Generate detailed ODP issue-level CSV")
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
    generate_detailed_issue_csv(args.output_dir, dataset_type="all")
