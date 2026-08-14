"""
Script to generate a plan status CSV, summarising endpoint presence against
expected dataset provisions for the local-plan collection.

The script:
- Retrieves all organisations with a statutory provision to provide plan datasets
- Fetches endpoint status from the reporting_latest_endpoints table
- Matches expected datasets (via pipelines) against actual endpoints
- Outputs a detailed CSV of provision vs. actual endpoint status
"""

import os
import pandas as pd
import argparse
from utils import datasette_query, datasette_query_paginated

# Dataset to Pipeline Map
ALL_PIPELINES = {
    "local-plan": [
        "local-plan",
        "minerals-plan",
        "plan-timetable",
        "supplementary-plan",
        "waste-plan",
    ],
}

# Data Retrieval Functions
def get_provisions():
    """
    Retrieves provision records showing which organisations have a statutory
    obligation to provide plan datasets under the local-plan specification.

    Returns:
        pd.DataFrame: Provision table including organisation names and datasets.
    """
    sql = """
        SELECT
            p.organisation,
            org.name as name
        FROM provision p
        INNER JOIN organisation org ON org.organisation = p.organisation
        WHERE p.specification = "local-plan"
        AND p.provision_reason = "statutory"
        GROUP BY p.organisation
    """
    return datasette_query("digital-land", sql)


def get_endpoints() -> pd.DataFrame:
    """
    Retrieves all endpoint reporting data using pagination.

    Returns:
        pd.DataFrame: Combined table of all endpoint metadata and status.
    """
    sql = """
        SELECT
            rle.organisation,
            rle.collection,
            rle.pipeline,
            rle.endpoint,
            rle.endpoint_url,
            rle.licence,
            rle.latest_status as status,
            rle.days_since_200,
            rle.latest_exception as exception,
            rle.resource,
            rle.latest_log_entry_date,
            rle.endpoint_entry_date,
            rle.endpoint_end_date,
            rle.resource_start_date,
            rle.resource_end_date
        FROM reporting_latest_endpoints rle
    """
    df = datasette_query_paginated("performance", sql, page_size=1000)
    if df.empty:
        return df

    # Normalise organisation codes (remove -eng suffix)
    df["organisation"] = df["organisation"].str.replace("-eng", "", regex=False)
    return df

# CSV Export Logic
def generate_plans_summary_csv(output_dir: str) -> str:
    """
    Generates a CSV file showing plan provision status by dataset, pipeline, and endpoint.

    Args:
        output_dir (str): Directory to save the CSV output.

    Returns:
        str: Path to the saved CSV file.
    """
    provisions = get_provisions()
    endpoints = get_endpoints()
    output_rows = []

    for _, row in provisions.iterrows():
        organisation = row["organisation"]
        name = row["name"]

        for collection, pipelines in ALL_PIPELINES.items():
            for pipeline in pipelines:
                match = endpoints[
                    (endpoints["organisation"] == organisation) &
                    (endpoints["pipeline"] == pipeline)
                ]

                if not match.empty:
                    # Endpoint(s) exist — add one row per match
                    for _, ep in match.iterrows():
                        output_rows.append({
                            "organisation": organisation,
                            "name": name,
                            "collection": collection,
                            "pipeline": pipeline,
                            "endpoint": ep["endpoint"],
                            "endpoint_url": ep["endpoint_url"],
                            "licence": ep["licence"],
                            "status": ep["status"],
                            "days_since_200": ep["days_since_200"],
                            "exception": ep["exception"],
                            "resource": ep["resource"],
                            "latest_log_entry_date": ep["latest_log_entry_date"],
                            "endpoint_entry_date": ep["endpoint_entry_date"],
                            "endpoint_end_date": ep["endpoint_end_date"],
                            "resource_start_date": ep["resource_start_date"],
                            "resource_end_date": ep["resource_end_date"],
                        })
                else:
                    # No endpoint — mark as missing
                    output_rows.append({
                        "organisation": organisation,
                        "name": name,
                        "collection": collection,
                        "pipeline": pipeline,
                        "endpoint": "No endpoint added",
                        "endpoint_url": "",
                        "licence": "",
                        "status": "",
                        "days_since_200": "",
                        "exception": "",
                        "resource": "",
                        "latest_log_entry_date": "",
                        "endpoint_entry_date": "",
                        "endpoint_end_date": "",
                        "resource_start_date": "",
                        "resource_end_date": "",
                    })

    # Convert output to DataFrame and save as CSV
    df_final = pd.DataFrame(output_rows)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "plan_status.csv")
    df_final.to_csv(output_path, index=False)
    print(f"CSV generated at {output_path} with {len(df_final)} rows")
    return output_path

# CLI Parser
def parse_args():
    """
    Parses command-line arguments for specifying the output directory.

    Returns:
        argparse.Namespace: Parsed args containing the output path.
    """
    parser = argparse.ArgumentParser(description="Datasette batch exporter")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs"
    )
    return parser.parse_args()

# Script Entry Point
if __name__ == "__main__":
    # Parse CLI arguments
    args = parse_args()
    output_directory = args.output_dir

    # Generate and save plan endpoint summary
    generate_plans_summary_csv(output_directory)
