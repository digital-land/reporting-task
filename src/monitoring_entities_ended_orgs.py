from itertools import islice
from urllib.parse import urlparse
import requests
import pandas as pd
import click
import argparse
import os
import time
import logging

# ---------------------------------------
# Config
# ---------------------------------------
BASE_DB = "https://datasette.planning.data.gov.uk/digital-land"  # DB path
BASE_HOST = "https://datasette.planning.data.gov.uk"             # domain root
UA = {"User-Agent": "Mozilla/5.0 (compatible; slug-fetcher/1.0)"}
WANTED = ["dataset", "entity", "entry_date", "reference", "name", "organisation_entity"]
logger = logging.getLogger(__name__)

# ---------------------------------------
# Helpers
# ---------------------------------------
def fetch_sql_df(base_url: str, sql: str, timeout: int = 60) -> pd.DataFrame:
    """
    Run SQL against a Datasette instance. Works for both:
      - base_url = https://host        -> uses /_.json
      - base_url = https://host/dbname -> uses /dbname.json
    """
    parsed = urlparse(base_url)
    has_db_path = parsed.path and parsed.path.strip("/") != ""
    endpoint = (
        base_url.rstrip("/") + ".json"    # e.g. .../digital-land.json
        if has_db_path
        else base_url.rstrip("/") + "/.json"  # e.g. ...gov.uk/.json
    )
    r = requests.get(
        endpoint,
        params={"sql": sql, "_shape": "array", "_size": "max"},
        timeout=timeout,
        headers=UA,
    )
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data) if data else pd.DataFrame()


def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def get_all_dataset_slugs(base_db: str = BASE_DB) -> list:
    """
    Pull dataset slugs from the dataset registry table.
    (This lists all registered datasets—even if they don't have an entity table.)
    """
    sql = "SELECT dataset FROM dataset"
    df = fetch_sql_df(base_db, sql)
    if df.empty or "dataset" not in df.columns:
        return []
    return sorted(df["dataset"].dropna().astype(str).unique().tolist())


def build_total_slug_df(slugs, base_host=BASE_HOST, wanted_cols=WANTED) -> pd.DataFrame:
    """
    For each slug, try {base_host}/{slug}/entity.csv?_stream=on.
    If available, read, normalise to wanted_cols, and concat.
    """
    frames = []
    for slug in slugs:
        url = f"{base_host}/{slug}/entity.csv?_stream=on"
        try:
            # quick existence probe; allow 403/405 (HEAD often blocked)
            head = requests.head(url, timeout=20, allow_redirects=True)
            if head.status_code not in (200, 403, 405):
                # no endpoint — skip
                continue

            # try CSV read directly
            df = pd.read_csv(url, low_memory=False)

            # ensure wanted columns exist
            for c in wanted_cols:
                if c not in df.columns:
                    df[c] = pd.NA

            # fill dataset with slug if missing/blank
            if df["dataset"].isna().all():
                df["dataset"] = slug
            else:
                df["dataset"] = df["dataset"].fillna(slug)

            frames.append(df[wanted_cols])

        except Exception:
            # skip problematic slug and continue
            continue
        finally:
            time.sleep(0.03) 

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=wanted_cols)


def parse_args():
    parser = argparse.ArgumentParser(description="Export merged entity/orgs data")
    parser.add_argument(
        "--output_dir",
        type=str,
        default="./outputs",   # default if not passed
        help="Directory where the output CSV should be saved",
    )
    return parser.parse_args()

def build_dataset(output_dir):
    # 1) Build the big entity table across all slugs
    slugs = get_all_dataset_slugs(BASE_DB)
    total_slug_df = build_total_slug_df(slugs, base_host=BASE_HOST, wanted_cols=WANTED)

    # 2) Organisations (ENDED ONLY: end_date IS NOT NULL)
    orgs_url = f"{BASE_HOST}/digital-land/organisation.csv?_stream=on"
    orgs_df = pd.read_csv(orgs_url, low_memory=False)

    ended_orgs_df = (
        orgs_df.loc[orgs_df["end_date"].notna(), ["name", "entity", "reference", "dataset", "end_date"]]
            .rename(columns={
                "name": "organisation_name",
                "entity": "organisation_entity",
                "end_date": "organisation_end_date",
            })
            .copy()
    )
    ended_orgs_df["organisation_code"] = ended_orgs_df["dataset"].astype(str) + ":" + ended_orgs_df["reference"].astype(str)
    ended_orgs_df = ended_orgs_df.drop(columns=["reference", "dataset"])

    # 3) Merge entities with ended orgs on organisation_entity
    merge_df = ended_orgs_df.merge(total_slug_df, how="inner", on="organisation_entity")

    # 4) Save to args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "entities_with_ended_orgs.csv")
    merge_df.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")

@click.command()
@click.option("--output-dir", required=True)
def build_dataset_cli(output_dir):
    build_dataset(output_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_dataset_cli()

    
    
