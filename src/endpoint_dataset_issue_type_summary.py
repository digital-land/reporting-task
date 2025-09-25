import pandas as pd
import os
import argparse

def full_datasette_table(tables, output_dir):
    """
    Downloads full tables from Datasette in CSV format using streaming.

    Args:
        tables (dict): A dictionary where keys are table names and values are their Datasette URLs.
        output_dir (str): The directory to save the exported CSV files.
    """
    os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists

    for name, url in tables.items():
        full_url = f"{url}.csv?_stream=on"  # Enable full streaming of rows
        try:
            df = pd.read_csv(full_url)  # Load full dataset
            csv_name = f"{name}.csv"
            save_path = os.path.join(output_dir, csv_name)
            df.to_csv(save_path, index=False)  # Save to CSV without index
            print(f"Saved: {save_path}")
        except Exception as e:
            print(f"[ERROR] Failed to fetch {name}: {e}")

def parse_args():
    """
    Parses command-line arguments for specifying the output directory.

    Returns:
        argparse.Namespace: Parsed arguments containing the output directory path.
    """
    parser = argparse.ArgumentParser(description="Datasette batch exporter")
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save exported CSVs"
    )
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_args()

    # Dictionary of table names and their Datasette URLs
    tables = {
        "endpoint-dataset-issue-type-summary":
            "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary"
    }

    # Run export
    full_datasette_table(tables, args.output_dir)
