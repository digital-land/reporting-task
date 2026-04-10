"""
Script to extract listed building end dates by organisation using listed-building-outline data.

Fetches listed-building and listed-building-outline datasets,
merges them to associate end dates with organisations, and enriches
with organisation names.
"""

import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

FILES_URL = os.environ.get("FILES_URL", "https://files.planning.data.gov.uk")

# CSV endpoints with streaming
## Using CSVs as the 'listed-building' column is only available in the CSV format
LISTED_BUILDING_URL = f"{FILES_URL}/dataset/listed-building.csv?_stream=on"
LISTED_BUILDING_OUTLINE_URL = f"{FILES_URL}/dataset/listed-building-outline.csv?_stream=on"
ORG_URL = "https://datasette.planning.data.gov.uk/digital-land/organisation.csv?_stream=on"


def main(output_dir: str):
    """
    Fetch and enrich listed building end date data with organisation information.

    Args:
        output_dir (str): Directory to save output CSV
    """

    # ---------------------------------------------------------------
    # Load listed building datasets
    # ---------------------------------------------------------------
    try:
        logger.info("Fetching listed-building data...")
        df_bo = pd.read_csv(LISTED_BUILDING_URL, low_memory=False)
        logger.info(f"Loaded {len(df_bo)} listed building records")

        logger.info("Fetching listed-building-outline data...")
        df_bo_outline = pd.read_csv(LISTED_BUILDING_OUTLINE_URL, low_memory=False)
        logger.info(f"Loaded {len(df_bo_outline)} listed building outline records")

    except Exception as e:
        logger.error(f"Failed to load listed building data: {e}")
        os.makedirs(output_dir, exist_ok=True)
        pd.DataFrame().to_csv(
            os.path.join(output_dir, "listed_building_end_date.csv"), index=False
        )
        return

    # ---------------------------------------------------------------
    # Normalize data types
    # ---------------------------------------------------------------
    # Ensure listed-building column is numeric (nullable int)
    df_bo_outline["listed-building"] = pd.to_numeric(
        df_bo_outline["listed-building"], errors="coerce"
    ).astype("Int64")

    # ---------------------------------------------------------------
    # Merge datasets on reference and listed-building ID
    # ---------------------------------------------------------------
    df_merged = pd.merge(
        df_bo[["reference", "end-date"]],
        df_bo_outline[["listed-building", "reference", "entity", "organisation-entity"]],
        left_on="reference",
        right_on="listed-building",
        how="inner",
        suffixes=("_lb", "_lbo"),
    )

    # Clean up columns - keep entity from listed-building-outline
    df_merged = df_merged[["reference_lbo", "entity", "organisation-entity", "end-date"]].rename(
        columns={"reference_lbo": "reference"}
    )

    # ---------------------------------------------------------------
    # Load and merge organisation data
    # ---------------------------------------------------------------
    try:
        df_org = pd.read_csv(ORG_URL, low_memory=False)
        df_org = df_org[["entity", "organisation"]].rename(columns={"entity": "organisation-entity"}).copy()

        df_final = pd.merge(
            df_merged,
            df_org,
            on="organisation-entity",
            how="left",
        )
    except Exception as e:
        logger.error(f"Failed to load organisation data: {e}")
        df_final = df_merged

    # ---------------------------------------------------------------
    # Filter to only rows with end dates, sort and save
    # ---------------------------------------------------------------
    df_final = df_final[df_final['end-date'].notna() & (df_final['end-date'] != '')]
    df_final = df_final.sort_values("organisation")

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "listed_building_end_date.csv")
    df_final[['reference', 'entity', 'end-date', 'organisation-entity', 'organisation']].rename(
        columns={'end-date': 'end_date', 'organisation-entity': 'organisation_entity'}
    ).to_csv(output_file, index=False)
    logger.info(f"Saved output to {output_file} with {len(df_final)} rows")


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract listed building end dates by organisation"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output directory for CSV file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)
