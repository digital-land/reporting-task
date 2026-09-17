import logging
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO
import pandas as pd

logger = logging.getLogger(__name__)

DATASETTE_URL = os.environ.get("DATASETTE_URL", "https://datasette.planning.data.gov.uk")

# datasette has occasionally returned a 400, or a 200 with an empty body, for requests that
# succeed a moment later when retried - neither is caught by get_http_session's transport-level
# retry (which only covers 502/503/504 and connection errors), so callers get a request-level
# retry here instead.
EMPTY_RESPONSE_RETRY_ATTEMPTS = 8
EMPTY_RESPONSE_RETRY_BACKOFF_SECONDS = 10


def get_http_session() -> requests.Session:
    """Returns a requests Session with retry for transient server errors (502, 503, 504)."""
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _is_retryable_response(response: requests.Response) -> bool:
    if response.status_code == 400:
        return True
    return response.status_code == 200 and not response.text.strip()


def get_with_retry(session: requests.Session, url: str, params: dict = None) -> requests.Response:
    """
    GET a URL, retrying (with backoff) on a 400 or an empty-bodied 200 - both observed from
    datasette as transient, alongside the 502/503/504s get_http_session already retries at the
    transport level. Raises the real HTTPError, or a RuntimeError naming the URL for a
    persistently empty body, once attempts are exhausted.

    Datasette sits behind CloudFront, which has been observed caching a broken empty response
    for a given query string and serving that same stale entry on every subsequent request -
    retrying the identical URL just hits the same cache entry, so retries add a cache-busting
    param to force a fresh request past the cache once the first attempt looks bad.
    """
    response = None
    for attempt in range(1, EMPTY_RESPONSE_RETRY_ATTEMPTS + 1):
        request_params = dict(params or {})
        if attempt > 1:
            request_params["_cb"] = f"{time.time()}-{attempt}"
        response = session.get(url, params=request_params)
        if not _is_retryable_response(response):
            break
        if attempt < EMPTY_RESPONSE_RETRY_ATTEMPTS:
            logger.warning(
                "Retrying %s after status %s empty/bad response (attempt %d/%d)",
                url, response.status_code, attempt, EMPTY_RESPONSE_RETRY_ATTEMPTS,
            )
            time.sleep(EMPTY_RESPONSE_RETRY_BACKOFF_SECONDS * attempt)

    if response.status_code == 200 and not response.text.strip():
        raise RuntimeError(f"Empty response from {url} after {EMPTY_RESPONSE_RETRY_ATTEMPTS} attempts")
    response.raise_for_status()
    return response


def read_csv_with_retry(url: str, **kwargs) -> pd.DataFrame:
    """Fetch a CSV from a URL with retry logic and parse into a DataFrame."""
    session = get_http_session()
    response = get_with_retry(session, url)
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
    response = get_with_retry(session, full_url, params=params)
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
        response = get_with_retry(session, url)
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
