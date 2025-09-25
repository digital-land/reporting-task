import pandas as pd
import ast
import argparse
import os

# Load expectations table
url = "https://datasette.planning.data.gov.uk/digital-land/expectation.csv?_stream=on"
df = pd.read_csv(url)
df = df[df["operation"] == "duplicate_geometry_check"]

# Parse 'details' column
def parse_details(val):
    try:
        return ast.literal_eval(val)
    except Exception:
        return {}

def main(output_dir):
    df["details_parsed"] = df["details"].apply(parse_details)

    # Extract match records
    records = []
    for _, row in df.iterrows():
        dataset = row["dataset"]
        operation = row["operation"]
        details = row["details_parsed"]

        # Complete matches
        for match in details.get("complete_matches", []):
            records.append({
                "dataset": dataset,
                "operation": operation,
                "message": "complete_match",
                "entity_a": match.get("entity_a"),
                "organisation_entity_a": match.get("organisation_entity_a"),
                "entity_b": match.get("entity_b"),
                "organisation_entity_b": match.get("organisation_entity_b"),
            })

        # Single matches
        for match in details.get("single_matches", []):
            records.append({
                "dataset": dataset,
                "operation": operation,
                "message": "single_match",
                "entity_a": match.get("entity_a"),
                "organisation_entity_a": match.get("organisation_entity_a"),
                "entity_b": match.get("entity_b"),
                "organisation_entity_b": match.get("organisation_entity_b"),
            })

    df_matches = pd.DataFrame(records)

    # Load entity tables
    url_map = {
        "conservation-area": "https://datasette.planning.data.gov.uk/conservation-area/entity.csv?_stream=on",
        "article-4-direction-area": "https://datasette.planning.data.gov.uk/article-4-direction-area/entity.csv?_stream=on",
        "listed-building-outline": "https://datasette.planning.data.gov.uk/listed-building-outline/entity.csv?_stream=on",
        "tree-preservation-zone": "https://datasette.planning.data.gov.uk/tree-preservation-zone/entity.csv?_stream=on",
        "tree": "https://datasette.planning.data.gov.uk/tree/entity.csv?_stream=on",
    }

    columns_to_keep = ["entity", "dataset", "end_date", "entry_date", "geometry", "name", "organisation_entity"]

    entity_tables = {}
    for dataset_name, entity_url in url_map.items():
        df_entity = pd.read_csv(entity_url)
        df_entity["dataset"] = dataset_name
        entity_tables[dataset_name] = df_entity[columns_to_keep].copy()

    # Combine all entity tables
    df_entities = pd.concat(entity_tables.values(), ignore_index=True)

    # Merge entity_a metadata
    df_matches = df_matches.merge(
        df_entities,
        how="left",
        left_on=["dataset", "entity_a"],
        right_on=["dataset", "entity"]
    ).rename(columns={
        "end_date": "entity_a_end_date",
        "entry_date": "entity_a_entry_date",
        "geometry": "entity_a_geometry",
        "name": "entity_a_name",
        "organisation_entity": "entity_a_organisation"
    }).drop(columns=["entity"])

    # Merge entity_b metadata
    df_matches = df_matches.merge(
        df_entities,
        how="left",
        left_on=["dataset", "entity_b"],
        right_on=["dataset", "entity"]
    ).rename(columns={
        "end_date": "entity_b_end_date",
        "entry_date": "entity_b_entry_date",
        "geometry": "entity_b_geometry",
        "name": "entity_b_name",
        "organisation_entity": "entity_b_organisation"
    }).drop(columns=["entity"])

    # Reorder columns
    ordered_cols = [
        "dataset", "operation", "message",
        "entity_a", "entity_a_name", "entity_a_organisation", "entity_a_entry_date", "entity_a_end_date", "entity_a_geometry",
        "entity_b", "entity_b_name", "entity_b_organisation", "entity_b_entry_date", "entity_b_end_date", "entity_b_geometry"
    ]
    df_matches = df_matches[ordered_cols]

    # Save CSVs
    os.makedirs(output_dir, exist_ok=True)
    matches_csv = os.path.join(output_dir, "duplicate_entity_expectation.csv")
    df_matches.to_csv(matches_csv, index=False)

def parse_args():
    parser = argparse.ArgumentParser(description="Duplicate geometry checker")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs"
    )
    return parser.parse_args()

# Entry point
if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)