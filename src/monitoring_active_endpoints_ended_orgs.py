import os
import argparse
import click
import pandas as pd

base_url = "https://datasette.planning.data.gov.uk"

def ended_orgs_active_endpoints(output_dir):
    
    # Organisations (ENDED ONLY: end_date IS NOT NULL)
    orgs_url = f"{base_url}/digital-land/organisation.csv?_stream=on"
    orgs_df = pd.read_csv(orgs_url)

    ended_orgs_df = (
        orgs_df.loc[orgs_df["end_date"].notna(), ["name", "entity", "reference", "dataset"]]
        .rename(columns={
            "name": "organisation_name",
            "entity": "organisation_entity",
        })
    )

    ended_orgs_df["organisation_code"] = ended_orgs_df["dataset"] + ":" + ended_orgs_df["reference"]
    ended_orgs_df = ended_orgs_df.drop(columns=["reference", "dataset"])

    # Endpoints (ACTIVE ONLY: endpoint_end_date IS NULL)
    endpoints_url = f"{base_url}/performance/reporting_historic_endpoints.csv?_stream=on"
    endpoints_df = pd.read_csv(endpoints_url, low_memory=False)

    active_endpoints_df = endpoints_df[endpoints_df["endpoint_end_date"].isna()].copy()

    # Keep only required fields (plus dataset name)
    active_endpoints_df = active_endpoints_df.rename(columns={"dataset": "dataset_name"})
    active_endpoints_df = active_endpoints_df[
        [
            "organisation",
            "dataset_name",
            "endpoint_url",
            "endpoint_entry_date",
            "endpoint_end_date",        # will be NaN for active, but required in output
            "latest_status",                   # latest log status
            "latest_log_entry_date",
        ]
    ]

    active_endpoints_df = active_endpoints_df.rename(columns={"organisation":"organisation_code","latest_status":"status"})

    # --- Merge on organisation_code to get "active endpoints from ended organisations"
    merged_df = ended_orgs_df.merge(active_endpoints_df, how="inner", on="organisation_code")

    # Final columns & types
    merged_df = merged_df[
        [
            "organisation_name",
            "organisation_entity",
            "organisation_code",
            "dataset_name",
            "endpoint_url",
            "endpoint_entry_date",
            "endpoint_end_date",
            "status",
            "latest_log_entry_date",
        ]
    ].copy()

    for c in ["endpoint_entry_date", "endpoint_end_date", "latest_log_entry_date"]:
        if c in merged_df.columns:
            merged_df[c] = pd.to_datetime(merged_df[c], errors="coerce")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "ended_orgs_active_endpoints.csv")

    merged_df.to_csv(output_path, index=False)

@click.command()
@click.option("--output-dir", required=True)
def build_dataset_cli(output_dir):
    ended_orgs_active_endpoints(output_dir)

if __name__ == "__main__":
    build_dataset_cli()