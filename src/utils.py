import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO
import pandas as pd

DATASETTE_URL = os.environ.get("DATASETTE_URL", "https://datasette.planning.data.gov.uk")


def get_http_session() -> requests.Session:
    """Returns a requests Session with retry for transient server errors (502, 503, 504)."""
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def read_csv_with_retry(url: str, **kwargs) -> pd.DataFrame:
    """Fetch a CSV from a URL with retry logic and parse into a DataFrame."""
    session = get_http_session()
    response = session.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text), **kwargs)


def datasette_query(db: str, sql: str, filter: dict = None, url: str = DATASETTE_URL) -> pd.DataFrame:
    """
    Executes an SQL query against a Datasette database and returns the result as a DataFrame.

    Uses Datasette's default query shape (not _shape=array) so that column names survive
    a zero-row result - with _shape=array, an empty result is a bare `[]` with no column
    metadata, which silently drops columns that callers may downstream merge/filter on.
    """
    full_url = f"{url}/{db}.json"
    params = {"sql": sql, "_size": "max"}
    if filter:
        params.update(filter)
    session = get_http_session()
    response = session.get(full_url, params=params)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data["rows"], columns=data["columns"])


def datasette_query_paginated(db: str, sql: str, page_size: int = 1000, url: str = DATASETTE_URL) -> pd.DataFrame:
    """Runs a Datasette query in LIMIT/OFFSET pages and concatenates the results."""
    frames = []
    offset = 0

    while True:
        page_sql = f"{sql}\nLIMIT {page_size} OFFSET {offset}"
        page_df = datasette_query(db, page_sql, url=url)
        if page_df.empty:
            break

        frames.append(page_df)

        if len(page_df) < page_size:
            break
        offset += page_size

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def follow_datasette_next_url(url: str, session: requests.Session = None) -> pd.DataFrame:
    """
    Fetch every page of a Datasette JSON list endpoint, following next_url.

    Datasette caps _size=max at 1000 rows per request regardless of how many rows
    match, so a single request silently truncates results rather than raising an
    error - the response's next_url must be followed until it's exhausted.
    """
    session = session or get_http_session()
    rows = []
    columns = None
    while url:
        response = session.get(url)
        response.raise_for_status()
        data = response.json()
        if columns is None:
            columns = data["columns"]
        rows.extend(data["rows"])
        url = data.get("next_url")
    return pd.DataFrame(rows, columns=columns)


def fetch_datasette_csv_table(table: str, db: str = "digital-land", url: str = DATASETTE_URL, **kwargs) -> pd.DataFrame:
    """Fetch a full Datasette table as a streamed CSV (e.g. organisation, endpoint, source)."""
    table_url = f"{url}/{db}/{table}.csv?_stream=on"
    return read_csv_with_retry(table_url, **kwargs)
