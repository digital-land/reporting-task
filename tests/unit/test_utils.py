import pytest
import requests

from utils import (
    DATASETTE_URL,
    EMPTY_RESPONSE_RETRY_ATTEMPTS,
    datasette_query,
    datasette_query_paginated,
    fetch_datasette_csv_table,
    follow_datasette_next_url,
    get_with_retry,
    read_csv_with_retry,
)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Every retry test below exercises real retry attempts - skip the actual backoff delay."""
    monkeypatch.setattr("utils.time.sleep", lambda seconds: None)


def test_read_csv_with_retry_parses_csv(requests_mock):
    requests_mock.get("https://example.com/data.csv", text="a,b\n1,2\n3,4\n")

    df = read_csv_with_retry("https://example.com/data.csv")

    assert list(df.columns) == ["a", "b"]
    assert df.to_dict("records") == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_datasette_query_sends_expected_params_and_parses_json(requests_mock):
    requests_mock.get(
        f"{DATASETTE_URL}/digital-land.json",
        json={"columns": ["organisation", "name"], "rows": [["gov:1", "Example"]]},
    )

    df = datasette_query("digital-land", "SELECT * FROM organisation")

    assert df.to_dict("records") == [{"organisation": "gov:1", "name": "Example"}]
    qs = requests_mock.last_request.qs
    assert qs["sql"] == ["select * from organisation"]
    assert qs["_size"] == ["max"]


def test_datasette_query_preserves_columns_on_zero_row_result(requests_mock):
    """A zero-row result must still carry column names, so callers can merge/filter on them."""
    requests_mock.get(
        f"{DATASETTE_URL}/digital-land.json",
        json={"columns": ["organisation"], "rows": []},
    )

    df = datasette_query("digital-land", "SELECT organisation FROM expectation WHERE 1=0")

    assert list(df.columns) == ["organisation"]
    assert df.empty


def test_datasette_query_merges_extra_filter_params(requests_mock):
    requests_mock.get(f"{DATASETTE_URL}/digital-land.json", json={"columns": [], "rows": []})

    datasette_query("digital-land", "SELECT 1", filter={"_size": "5"})

    assert requests_mock.last_request.qs["_size"] == ["5"]


def test_datasette_query_paginated_concatenates_full_pages_and_stops_on_short_page(requests_mock):
    page_one = {"columns": ["id"], "rows": [[0], [1]]}
    page_two = {"columns": ["id"], "rows": [[2]]}
    requests_mock.get(
        f"{DATASETTE_URL}/digital-land.json",
        [{"json": page_one}, {"json": page_two}],
    )

    df = datasette_query_paginated("digital-land", "SELECT id FROM thing", page_size=2)

    assert df["id"].tolist() == [0, 1, 2]
    assert requests_mock.call_count == 2


def test_datasette_query_paginated_returns_empty_frame_when_first_page_empty(requests_mock):
    requests_mock.get(f"{DATASETTE_URL}/digital-land.json", json={"columns": ["id"], "rows": []})

    df = datasette_query_paginated("digital-land", "SELECT id FROM thing")

    assert df.empty
    assert requests_mock.call_count == 1


def test_follow_datasette_next_url_follows_pages_until_exhausted(requests_mock):
    first_url = "https://example.com/digital-land/expectation.json"
    second_url = "https://example.com/digital-land/expectation.json?_next=abc"
    requests_mock.get(
        first_url,
        json={"columns": ["id"], "rows": [[1], [2]], "next_url": second_url},
    )
    requests_mock.get(
        second_url,
        json={"columns": ["id"], "rows": [[3]], "next_url": None},
    )

    df = follow_datasette_next_url(first_url)

    assert list(df.columns) == ["id"]
    assert df["id"].tolist() == [1, 2, 3]


def test_fetch_datasette_csv_table_builds_streamed_table_url(requests_mock):
    requests_mock.get(
        f"{DATASETTE_URL}/digital-land/organisation.csv?_stream=on",
        text="entity,name\n1,Example\n",
    )

    df = fetch_datasette_csv_table("organisation")

    assert df.to_dict("records") == [{"entity": 1, "name": "Example"}]


def test_fetch_datasette_csv_table_respects_db_argument(requests_mock):
    requests_mock.get(
        f"{DATASETTE_URL}/performance/reporting_historic_endpoints.csv?_stream=on",
        text="endpoint\nabc\n",
    )

    df = fetch_datasette_csv_table("reporting_historic_endpoints", db="performance")

    assert df.to_dict("records") == [{"endpoint": "abc"}]


def test_get_with_retry_retries_on_empty_200_then_succeeds(requests_mock):
    requests_mock.get(
        "https://example.com/data.csv",
        [{"text": ""}, {"text": ""}, {"text": "a,b\n1,2\n"}],
    )
    session = requests.Session()

    response = get_with_retry(session, "https://example.com/data.csv")

    assert response.text == "a,b\n1,2\n"
    assert requests_mock.call_count == 3


def test_get_with_retry_retries_on_400_then_succeeds(requests_mock):
    requests_mock.get(
        "https://example.com/data.csv",
        [{"status_code": 400, "text": ""}, {"status_code": 200, "text": "a,b\n1,2\n"}],
    )
    session = requests.Session()

    response = get_with_retry(session, "https://example.com/data.csv")

    assert response.status_code == 200
    assert requests_mock.call_count == 2


def test_get_with_retry_raises_runtime_error_after_exhausting_attempts_on_persistent_empty_body(requests_mock):
    requests_mock.get("https://example.com/data.csv", text="")
    session = requests.Session()

    expected = f"Empty response from https://example.com/data.csv after {EMPTY_RESPONSE_RETRY_ATTEMPTS} attempts"
    with pytest.raises(RuntimeError, match=expected):
        get_with_retry(session, "https://example.com/data.csv")

    assert requests_mock.call_count == EMPTY_RESPONSE_RETRY_ATTEMPTS


def test_get_with_retry_raises_http_error_after_exhausting_attempts_on_persistent_400(requests_mock):
    requests_mock.get("https://example.com/data.csv", status_code=400, text="bad request")
    session = requests.Session()

    with pytest.raises(requests.exceptions.HTTPError):
        get_with_retry(session, "https://example.com/data.csv")

    assert requests_mock.call_count == EMPTY_RESPONSE_RETRY_ATTEMPTS


def test_get_with_retry_adds_cache_busting_param_on_retry_but_not_first_attempt(requests_mock):
    """Regression test: a CDN caching a broken response for a given query string will keep
    serving that same stale entry on identical retries, so retries must vary the request."""
    requests_mock.get(
        "https://example.com/data.csv",
        [{"text": ""}, {"text": "a,b\n1,2\n"}],
    )
    session = requests.Session()

    get_with_retry(session, "https://example.com/data.csv")

    assert "_cb" not in requests_mock.request_history[0].qs
    assert "_cb" in requests_mock.request_history[1].qs


def test_get_with_retry_does_not_retry_on_immediate_success(requests_mock):
    requests_mock.get("https://example.com/data.csv", text="a,b\n1,2\n")
    session = requests.Session()

    get_with_retry(session, "https://example.com/data.csv")

    assert requests_mock.call_count == 1


def test_read_csv_with_retry_recovers_from_one_empty_response(requests_mock):
    """Regression test for the EmptyDataError seen in production: an empty response followed
    by a real one should now succeed instead of pandas.errors.EmptyDataError bubbling up."""
    requests_mock.get(
        "https://example.com/data.csv",
        [{"text": ""}, {"text": "a,b\n1,2\n"}],
    )

    df = read_csv_with_retry("https://example.com/data.csv")

    assert df.to_dict("records") == [{"a": 1, "b": 2}]
