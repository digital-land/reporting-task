import requests
import pandas as pd
import urllib.parse
import os
import argparse

def sql_queried_datasette_tables(urls: dict, sqls: list, save_dir: str):
    """
    Fetches data from a dictionary of Datasette URLs using optional SQL queries
    and saves each result as a CSV file in the specified directory.

    Args:
        urls (dict): Mapping of table names to Datasette base URLs.
        sqls (list of str): SQL queries corresponding to each URL.
        save_dir (str): Directory path where the resulting CSV files will be saved.

    Raises:
        ValueError: If the lengths of the URLs and SQL lists do not match.
    """
    if len(urls) != len(sqls):
        raise ValueError("The number of URLs and SQL queries must match.")

    # Ensure the output directory exists
    os.makedirs(save_dir, exist_ok=True)

    # Iterate over each (name, URL) and associated SQL
    for (name, url), sql in zip(urls.items(), sqls):
        try:
            # Define the output CSV filename
            csv_name = f"{name}.csv"

            # Encode SQL query and construct JSON API URL
            encoded_sql = urllib.parse.quote(sql)
            full_url = f"{url}.json?sql={encoded_sql}&_shape=array"
            
            print(f"Fetching: {name} from SQL URL:\n{full_url}")

            # Fetch JSON data and load into DataFrame
            response = requests.get(full_url)
            response.raise_for_status()
            data = response.json()
            print(f"Rows returned: {len(data)}")
            df = pd.DataFrame(data)

            # rename columns to match expected
            df.rename(columns={'entry-date': 'entry_date'}, inplace=True)

            # Save DataFrame to CSV in the specified directory
            save_path = os.path.join(save_dir, csv_name)
            df.to_csv(save_path, index=False)
            print(f"Saved: {save_path}")

        except Exception as e:
            # Log failure and continue
            print(f"Failed to fetch from {url}: {e}")

def parse_args():
    """
    Parses command-line arguments for the output directory.

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
    # Parse arguments from CLI
    args = parse_args()

    # Define URLs and SQL queries to export
    urls = {
        "operational_issues": "https://datasette.planning.data.gov.uk/digital-land"
    }

    sqls = [
        # SQL to count operational issues by week over the last 6 months
        """
        SELECT
            [entry-date],
            COUNT(rowid) AS issue_count
        FROM
            operational_issue
        WHERE
            [entry-date] >= DATE('now', '-6 months')
        GROUP BY
            [entry-date];
        """
    ]

    # Execute the export
    sql_queried_datasette_tables(urls, sqls, args.output_dir)
