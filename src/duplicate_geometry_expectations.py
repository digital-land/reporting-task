import pandas as pd
import ast
import argparse
import os
import logging
logger = logging.getLogger(__name__)

FILES_URL = os.environ.get("FILES_URL", "https://files.planning.data.gov.uk")

# Load expectations table
EXPECTATIONS_URL = "https://datasette.planning.data.gov.uk/digital-land/expectation.csv?_stream=on"

# Entity tables to enrich A/B sides
ENTITY_URLS = {
    "conservation-area": f"{FILES_URL}/dataset/conservation-area.parquet",
    "article-4-direction-area": f"{FILES_URL}/dataset/article-4-direction-area.parquet",
    "listed-building-outline": f"{FILES_URL}/dataset/listed-building-outline.parquet",
    "tree-preservation-zone": f"{FILES_URL}/dataset/tree-preservation-zone.parquet",
    "tree": f"{FILES_URL}/dataset/tree.parquet",
}

# Orgs lookup
ORGS_URL = "https://files.planning.data.gov.uk/organisation-collection/dataset/organisation.csv"
LOOKUP_URLS = {
    "conservation-area": "https://raw.githubusercontent.com/digital-land/config/refs/heads/main/pipeline/conservation-area/lookup.csv",
    "article-4-direction-area": "https://raw.githubusercontent.com/digital-land/config/refs/heads/main/pipeline/article-4-direction/lookup.csv",
    "listed-building-outline": "https://raw.githubusercontent.com/digital-land/config/refs/heads/main/pipeline/listed-building/lookup.csv",
    "tree-preservation-zone": "https://raw.githubusercontent.com/digital-land/config/refs/heads/main/pipeline/tree-preservation-order/lookup.csv",
    "tree": "https://raw.githubusercontent.com/digital-land/config/refs/heads/main/pipeline/tree-preservation-order/lookup.csv",
}

# Load provision table to check if LPA is in ODP
ODP_URL = "https://datasette.planning.data.gov.uk/digital-land/provision.csv?_stream=on"


def parse_details(val):
    try:
        return ast.literal_eval(val)
    except Exception:
        return {}


def main(output_dir: str):
    # ------------------------------------------------------------
    # Load and filter expectations
    # ------------------------------------------------------------
    df = pd.read_csv(EXPECTATIONS_URL, low_memory=False)
    df = df[df["operation"] == "duplicate_geometry_check"].copy()
    if df.empty:
        os.makedirs(output_dir, exist_ok=True)
        out = os.path.join(output_dir, "duplicate_entity_expectation.csv")
        pd.DataFrame().to_csv(out, index=False)
        return

    df["details_parsed"] = df["details"].apply(parse_details)

    # ------------------------------------------------------------
    # Extract match records
    # ------------------------------------------------------------
    records = []
    for _, row in df.iterrows():
        dataset = row["dataset"]
        operation = row["operation"]
        details = row["details_parsed"] or {}

        for match in details.get("complete_matches", []):
            records.append(
                {
                    "dataset": dataset,
                    "operation": operation,
                    "message": "complete_match",
                    "entity_a": match.get("entity_a"),
                    "organisation_entity_a": match.get("organisation_entity_a"),
                    "entity_b": match.get("entity_b"),
                    "organisation_entity_b": match.get("organisation_entity_b"),
                }
            )
        for match in details.get("single_matches", []):
            records.append(
                {
                    "dataset": dataset,
                    "operation": operation,
                    "message": "single_match",
                    "entity_a": match.get("entity_a"),
                    "organisation_entity_a": match.get("organisation_entity_a"),
                    "entity_b": match.get("entity_b"),
                    "organisation_entity_b": match.get("organisation_entity_b"),
                }
            )

    df_matches = pd.DataFrame.from_records(records)

    # Bail early if no matches
    if df_matches.empty:
        os.makedirs(output_dir, exist_ok=True)
        out = os.path.join(output_dir, "duplicate_entity_expectation.csv")
        df_matches.to_csv(out, index=False)
        return

    # ------------------------------------------------------------
    # Load & prep entity tables
    # ------------------------------------------------------------
    cols = [
        "entity",
        "dataset",
        "end_date",
        "entry_date",
        "geometry",
        "name",
        "organisation_entity",
    ]
    entity_tbls = []
    for dataset_name, entity_url in ENTITY_URLS.items():
        try:
            t = pd.read_parquet(entity_url)
            t.columns = t.columns.str.replace('-', '_')
            # Ensure required columns exist (skip missing datasets)
            missing = [c for c in ["entity", "end_date", "entry_date", "geometry", "name", "organisation_entity"] if c not in t.columns]
            if missing:
                continue
            t = t[["entity", "end_date", "entry_date", "geometry", "name", "organisation_entity","dataset"]]
            # Normalize key types almost like you could do this on import
            t["entity"] = pd.to_numeric(t["entity"], errors="coerce").astype("Int64")
            t["organisation_entity"] = pd.to_numeric(t["organisation_entity"], errors="coerce").astype("Int64")
            entity_tbls.append(t[cols].copy())
        except Exception as e:
            logger.error(f"Failed to load entity table for dataset: {dataset_name} from {entity_url}")
            raise e


    if not entity_tbls:
        # No enrichment possible, just save what we have
        os.makedirs(output_dir, exist_ok=True)
        out = os.path.join(output_dir, "duplicate_entity_expectation.csv")
        df_matches.to_csv(out, index=False)
        return

    df_entities = pd.concat(entity_tbls, ignore_index=True)

    # ------------------------------------------------------------
    # Load orgs lookup
    # ------------------------------------------------------------
    df_orgs = (
        pd.read_csv(ORGS_URL, low_memory=False)[["entity", "name"]]
        .rename(columns={"entity": "organisation_entity", "name": "organisation_name"})
        .copy()
    )
    df_orgs["organisation_entity"] = pd.to_numeric(df_orgs["organisation_entity"], errors="coerce").astype("Int64")

    # ------------------------------------------------------------
    # Normalize match key dtypes
    # ------------------------------------------------------------
    for c in ["entity_a", "entity_b", "organisation_entity_a", "organisation_entity_b"]:
        if c in df_matches.columns:
            df_matches[c] = pd.to_numeric(df_matches[c], errors="coerce").astype("Int64")

    # ------------------------------------------------------------
    # Merge metadata for A
    # ------------------------------------------------------------
    entA = df_entities.add_prefix("entity_a_")
    df_matches = df_matches.merge(
        entA,
        how="left",
        left_on=["dataset", "entity_a"],
        right_on=["entity_a_dataset", "entity_a_entity"],
        validate="m:1",
        suffixes=("", "_drop"),
    )

    # Orgs for A
    df_matches = df_matches.merge(
        df_orgs.rename(columns={"organisation_name": "entity_a_organisation_name"}),
        how="left",
        left_on="entity_a_organisation_entity",
        right_on="organisation_entity",
        validate="m:1",
        suffixes=("", "_orgA"),
    )

    # ------------------------------------------------------------
    # Merge metadata for B
    # ------------------------------------------------------------
    entB = df_entities.add_prefix("entity_b_")
    df_matches = df_matches.merge(
        entB,
        how="left",
        left_on=["dataset", "entity_b"],
        right_on=["entity_b_dataset", "entity_b_entity"],
        validate="m:1",
        suffixes=("", "_dropB"),
    )

    # Orgs for B
    df_matches = df_matches.merge(
        df_orgs.rename(columns={"organisation_name": "entity_b_organisation_name"}),
        how="left",
        left_on="entity_b_organisation_entity",
        right_on="organisation_entity",
        validate="m:1",
        suffixes=("", "_orgB"),
    )

    # ------------------------------------------------------------
    # Create stable shorthand org columns (so they don't vanish)
    # Prefer the enriched *_organisation_entity; fall back to originals
    # ------------------------------------------------------------
    df_matches["entity_a_organisation"] = (
        df_matches.get("entity_a_organisation_entity").combine_first(df_matches.get("organisation_entity_a"))
    )
    df_matches["entity_b_organisation"] = (
        df_matches.get("entity_b_organisation_entity").combine_first(df_matches.get("organisation_entity_b"))
    )

    # ------------------------------------------------------------
    # Merge in organisations from lookup
    # ------------------------------------------------------------
    # Get unique datasets that have matches
    datasets_in_matches = df_matches["dataset"].unique()

    # Process each dataset separately
    results = []
    for dataset in datasets_in_matches:
        df_subset = df_matches[df_matches["dataset"] == dataset].copy()
        
        # Load the appropriate lookup for this dataset
        if dataset in LOOKUP_URLS:
            try:
                df_lookup = pd.read_csv(LOOKUP_URLS[dataset])
                df_lookup = df_lookup[["organisation", "entity"]].drop_duplicates(subset=["entity"], keep="first").copy()
                
                # Merge for entity_a
                df_subset = df_subset.merge(
                    df_lookup.rename(columns={"organisation": "lookup-org-a"}),
                    how="left",
                    left_on="entity_a",
                    right_on="entity",
                    validate="m:1",
                )
                # Merge for entity_b
                df_subset = df_subset.merge(
                    df_lookup.rename(columns={"organisation": "lookup-org-b"}),
                    how="left",
                    left_on="entity_b",
                    right_on="entity",
                    validate="m:1",
                )
            except Exception as e:
                logger.error(f"Failed to load lookup for {dataset}: {e}")
        
        results.append(df_subset)

    df_matches = pd.concat(results, ignore_index=True)

    # ------------------------------------------------------------
    # Clean up helper columns if present
    # ------------------------------------------------------------
    drop_cols = [
        "entity_a_dataset",
        "entity_b_dataset",
        "organisation_entity_orgA",
        "organisation_entity_orgB",
    ]
    for c in drop_cols:
        if c in df_matches.columns:
            df_matches.drop(columns=[c], inplace=True)

    # ------------------------------------------------------------
    # Create comparison column
    # ------------------------------------------------------------
    df_matches["lookup-same-org"] = df_matches["lookup-org-a"] == df_matches["lookup-org-b"]

    # ------------------------------------------------------------
    # Check if entity B organisation is in ODP
    # ------------------------------------------------------------
    try:
        df_provision = pd.read_csv(ODP_URL, low_memory=False)
        # Get organisations that are in the open-digital-planning project
        odp_orgs = set(df_provision[df_provision["project"] == "open-digital-planning"]["organisation"].unique())
        df_matches["in-odp"] = df_matches["lookup-org-b"].isin(odp_orgs)
    except Exception as e:
        logger.error(f"Failed to load ODP provision data: {e}")
        df_matches["in-odp"] = False

    # ------------------------------------------------------------
    # Final column order (only keep those that exist)
    # ------------------------------------------------------------
    ordered = [
        "dataset",
        "operation",
        "message",
        "entity_a",
        "entity_a_name",
        "entity_a_organisation",
        "entity_a_organisation_name",
        "entity_a_entry_date",
        "entity_a_end_date",
        "entity_a_geometry",
        "entity_b",
        "entity_b_name",
        "entity_b_organisation",
        "entity_b_organisation_name",
        "entity_b_entry_date",
        "entity_b_end_date",
        "entity_b_geometry",
        "organisation_entity_a", # keep originals for auditing
        "organisation_entity_b", # keep originals for auditing
        "lookup-org-a",
        "lookup-org-b",
        "lookup-same-org",
        "in-odp"
    ]
    ordered = [c for c in ordered if c in df_matches.columns]
    df_matches = df_matches[ordered].copy()
    df_matches.drop(columns=["organisation_entity_a", "organisation_entity_b"], inplace=True)

    # ------------------------------------------------------------
    # Save
    # ------------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    out_csv = os.path.join(output_dir, "duplicate_entity_expectation.csv")
    df_matches.to_csv(out_csv, index=False)


def parse_args():
    parser = argparse.ArgumentParser(description="Duplicate geometry checker – extract and enrich duplicates")
    parser.add_argument("--output-dir", type=str, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)