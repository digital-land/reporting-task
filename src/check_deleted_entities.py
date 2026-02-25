import pandas as pd
import json
import os
import logging
import requests

logger = logging.getLogger(__name__)

FILES_URL = os.environ.get("FILES_URL", "https://files.planning.data.gov.uk")

# URLs for data sources
ENDPOINT_URL = "https://datasette.planning.data.gov.uk/digital-land/expectation.json?passed__exact=False&operation__exact=count_deleted_entities&_sort=rowid&_size=max"
ORG_URL = "https://datasette.planning.data.gov.uk/digital-land/organisation.csv?_stream=on"


def main(output_dir: str):
    """
    Fetch deleted entities from expectations, enrich with entity metadata from parquet files.
    """

    # ---------------------------------------------------------------
    # Load and filter expectations
    # ---------------------------------------------------------------
    response = requests.get(ENDPOINT_URL)
    data = response.json()
    df = pd.DataFrame(data['rows'], columns=data['columns'])
    df_filtered = df[['dataset', 'organisation', 'details']].copy()

    # Parse JSON and extract entities list
    df_filtered['entities'] = df_filtered['details'].apply(
        lambda x: json.loads(x)['entities']
    )

    # Explode to one entity per row
    df_expanded = df_filtered.explode('entities')[
        ['dataset', 'organisation', 'entities']
    ].copy()
    df_expanded = df_expanded.reset_index(drop=True)

    #print(f"Found {len(df_expanded)} entities across {df_expanded['dataset'].nunique()} datasets")

    # ---------------------------------------------------------------
    # Load and merge organisation data
    # ---------------------------------------------------------------
    df_org = pd.read_csv(ORG_URL)
    df_org = df_org[['entity', 'organisation', 'name']].copy()
    df_org = df_org.rename(
        columns={
            'entity': 'organisation-entity',
            'name': 'organisation-name'
        }
    )

    df_final = pd.merge(df_expanded, df_org, on='organisation', how='left')
    df_final = df_final.rename(columns={'entities': 'entity'})

    # ---------------------------------------------------------------
    # Load entity data from parquet files
    # ---------------------------------------------------------------
    # Get unique datasets and build URLs dynamically
    unique_datasets = df_final['dataset'].unique()
    ENTITY_URLS = {
        dataset: f"{FILES_URL}/dataset/{dataset}.parquet"
        for dataset in unique_datasets
    }

    entity_dfs = []
    for dataset_name, url in ENTITY_URLS.items():
        try:
            df_entity = pd.read_parquet(url)

            # Check for required columns
            if 'entity' not in df_entity.columns or 'name' not in df_entity.columns:
                continue

            # Keep only needed columns
            cols_to_keep = ['entity', 'name']
            if 'reference' in df_entity.columns:
                cols_to_keep.append('reference')
            else:
                # If reference column doesn't exist, add empty column
                df_entity['reference'] = ''
                cols_to_keep.append('reference')

            df_entity = df_entity[cols_to_keep].copy()
            df_entity['dataset'] = dataset_name
            entity_dfs.append(df_entity)

        except Exception as e:
            logger.error(f"Failed to load {dataset_name}: {e}")

    if not entity_dfs:
        logger.error("No entity datasets loaded successfully")
        os.makedirs(output_dir, exist_ok=True)
        df_final.to_csv(
            os.path.join(output_dir, 'deleted_entities.csv'),
            index=False
        )
        return

    # ---------------------------------------------------------------
    # Combine entity data and merge
    # ---------------------------------------------------------------
    df_entities = pd.concat(entity_dfs, ignore_index=True)

    # Normalize entity IDs to numeric for consistent merging
    df_entities['entity'] = pd.to_numeric(
        df_entities['entity'], errors='coerce'
    ).astype('Int64')
    df_final['entity'] = pd.to_numeric(
        df_final['entity'], errors='coerce'
    ).astype('Int64')

    # Merge with entity metadata
    df_final = df_final.merge(
        df_entities,
        how='left',
        on=['dataset', 'entity'],
        validate='m:1',
        suffixes=('', '_entity')
    )

    # ---------------------------------------------------------------
    # Select and order final columns
    # ---------------------------------------------------------------
    final_cols = [
        'dataset',
        'entity',
        'organisation',
        'organisation-name',
        'organisation-entity',
        'name',
        'reference'
    ]

    df_final = df_final[final_cols].copy()

    # ---------------------------------------------------------------
    # Save output
    # ---------------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'deleted_entities.csv')
    df_final.to_csv(output_file, index=False)

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch and enrich deleted entities from expectations"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output directory for CSV file"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.output_dir)