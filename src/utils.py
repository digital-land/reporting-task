import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO
import pandas as pd


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
