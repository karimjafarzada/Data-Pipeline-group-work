"""Tests for the Socrata API client. The API is always mocked with `responses`."""

import logging
from datetime import date

import pytest
import requests
import responses
from responses import matchers

from pipeline.ingestion.client import (
    ApiConfigError,
    ApiRateLimitError,
    ApiRequestError,
    ApiResponseError,
    ApiServerError,
    SocrataClient,
)

BASE_URL = "https://api.test"
URL = f"{BASE_URL}/resource/ijzp-q8t2.json"


def records(*ids):
    """Records shaped like the API's: every value is a string."""
    return [{"id": str(i), "case_number": f"JA{i}"} for i in ids]


@pytest.fixture
def mocked_api():
    # Any request that isn't registered raises, so no test can reach the real API.
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def sleeps():
    """Every wait the client asks for, recorded instead of really sleeping."""
    return []


@pytest.fixture
def make_client(sleeps):
    def _make(**kwargs):
        kwargs.setdefault("sleep", sleeps.append)
        return SocrataClient(BASE_URL, **kwargs)

    return _make


@pytest.fixture
def client(make_client):
    return make_client()


# --- query parameters -------------------------------------------------------


def test_fetch_page_uses_default_page_size_and_orders_by_id(mocked_api, client):
    mocked_api.get(
        URL,
        json=records(1, 2),
        match=[matchers.query_param_matcher({"$limit": "5000", "$order": "id"})],
    )

    assert client.fetch_page() == records(1, 2)


def test_fetch_page_filters_after_id_and_dates_with_inclusive_end(mocked_api, client):
    mocked_api.get(
        URL,
        json=[],
        match=[
            matchers.query_param_matcher(
                {
                    "$limit": "100",
                    "$order": "id",
                    "$where": "id > 42"
                    " AND date >= '2024-01-01T00:00:00'"
                    " AND date < '2024-02-01T00:00:00'",
                }
            )
        ],
    )

    client.fetch_page(
        after_id="42",
        limit=100,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_fetch_page_accepts_only_a_start_date(mocked_api, client):
    mocked_api.get(
        URL,
        json=[],
        match=[
            matchers.query_param_matcher(
                {"$limit": "5000", "$order": "id", "$where": "date >= '2024-03-05T00:00:00'"}
            )
        ],
    )

    client.fetch_page(start_date=date(2024, 3, 5))


def test_page_size_is_configurable(mocked_api, make_client):
    mocked_api.get(
        URL,
        json=[],
        match=[matchers.query_param_matcher({"$limit": "250", "$order": "id"})],
    )

    make_client(page_size=250).fetch_page()


def test_every_request_has_a_timeout(mocked_api, make_client):
    mocked_api.get(URL, json=[])

    make_client(timeout=12).fetch_page()

    assert mocked_api.calls[0].request.req_kwargs["timeout"] == 12


def test_start_date_after_end_date_is_rejected(client):
    with pytest.raises(ValueError, match="after end_date"):
        client.fetch_page(start_date=date(2024, 2, 1), end_date=date(2024, 1, 1))


# --- app token --------------------------------------------------------------


def test_app_token_is_sent_as_header(mocked_api, make_client):
    mocked_api.get(
        URL, json=[], match=[matchers.header_matcher({"X-App-Token": "secret-token"})]
    )

    make_client(app_token="secret-token").fetch_page()


def test_no_app_token_header_without_token(mocked_api, client):
    mocked_api.get(URL, json=[])

    client.fetch_page()

    assert "X-App-Token" not in mocked_api.calls[0].request.headers


# --- configuration from the environment -------------------------------------


def test_from_env_reads_base_url_and_token(mocked_api, monkeypatch, sleeps):
    monkeypatch.setenv("API_BASE_URL", BASE_URL + "/")
    monkeypatch.setenv("SOCRATA_APP_TOKEN", "env-token")
    mocked_api.get(
        URL, json=[], match=[matchers.header_matcher({"X-App-Token": "env-token"})]
    )

    client = SocrataClient.from_env(sleep=sleeps.append)
    client.fetch_page()

    assert client.url == URL


def test_from_env_reads_the_incremental_timeout(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", BASE_URL)
    monkeypatch.setenv("API_INCREMENTAL_TIMEOUT_SECONDS", "45")

    assert SocrataClient.from_env().incremental_timeout == 45.0


def test_incremental_timeout_defaults_to_120_seconds(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", BASE_URL)
    monkeypatch.delenv("API_INCREMENTAL_TIMEOUT_SECONDS", raising=False)

    assert SocrataClient.from_env().incremental_timeout == 120.0


@pytest.mark.parametrize("bad", ["abc", "0", "-5", "inf"])
def test_from_env_rejects_a_bad_incremental_timeout(monkeypatch, bad):
    monkeypatch.setenv("API_BASE_URL", BASE_URL)
    monkeypatch.setenv("API_INCREMENTAL_TIMEOUT_SECONDS", bad)

    with pytest.raises(ApiConfigError, match="API_INCREMENTAL_TIMEOUT_SECONDS"):
        SocrataClient.from_env()


def test_from_env_without_base_url_raises_clear_error(monkeypatch):
    monkeypatch.delenv("API_BASE_URL", raising=False)

    with pytest.raises(ApiConfigError, match="API_BASE_URL is not set"):
        SocrataClient.from_env()


# --- responses we can't use -------------------------------------------------


def test_client_error_raises_request_error(mocked_api, client):
    mocked_api.get(URL, status=400, json={"message": "bad query"})

    with pytest.raises(ApiRequestError, match="HTTP 400"):
        client.fetch_page()


def test_response_that_is_not_a_list_raises(mocked_api, client):
    mocked_api.get(URL, json={"message": "unexpected"})

    with pytest.raises(ApiResponseError, match="got dict"):
        client.fetch_page()


def test_response_that_is_not_json_raises(mocked_api, client):
    mocked_api.get(URL, body="<html>oops</html>")

    with pytest.raises(ApiResponseError, match="not valid JSON"):
        client.fetch_page()


# --- paging -----------------------------------------------------------------


def page_matcher(where=None, limit="3"):
    params = {"$limit": limit, "$order": "id"}
    if where is not None:
        params["$where"] = where
    return [matchers.query_param_matcher(params)]


def test_iter_pages_stops_on_short_page(mocked_api, make_client):
    mocked_api.get(URL, json=records(1, 2, 3), match=page_matcher())
    mocked_api.get(URL, json=records(4, 5, 6), match=page_matcher("id > 3"))
    mocked_api.get(URL, json=records(7), match=page_matcher("id > 6"))

    pages = list(make_client(page_size=3).iter_pages())

    assert pages == [records(1, 2, 3), records(4, 5, 6), records(7)]
    assert len(mocked_api.calls) == 3


def test_iter_pages_skips_the_empty_last_page(mocked_api, make_client):
    mocked_api.get(URL, json=records(1, 2, 3), match=page_matcher())
    mocked_api.get(URL, json=[], match=page_matcher("id > 3"))

    pages = list(make_client(page_size=3).iter_pages())

    assert pages == [records(1, 2, 3)]


def test_iter_pages_keeps_the_date_filter_on_every_page(mocked_api, make_client):
    dates = "date >= '2024-01-01T00:00:00' AND date < '2024-01-02T00:00:00'"
    mocked_api.get(URL, json=records(1, 2, 3), match=page_matcher(dates))
    mocked_api.get(URL, json=records(4), match=page_matcher(f"id > 3 AND {dates}"))

    pages = list(
        make_client(page_size=3).iter_pages(
            start_date=date(2024, 1, 1), end_date=date(2024, 1, 1)
        )
    )

    assert pages == [records(1, 2, 3), records(4)]


def test_iter_pages_resumes_from_after_id(mocked_api, make_client):
    mocked_api.get(URL, json=records(501, 502), match=page_matcher("id > 500"))

    pages = list(make_client(page_size=3).iter_pages(after_id=500))

    assert pages == [records(501, 502)]


def test_iter_pages_stops_if_paging_does_not_move_forward(mocked_api, make_client):
    # A broken API that ignores the id filter would otherwise loop forever.
    mocked_api.get(URL, json=records(1, 2, 3), match=page_matcher("id > 10"))

    with pytest.raises(ApiResponseError, match="did not move forward"):
        list(make_client(page_size=3).iter_pages(after_id=10))


# --- paging by change (incremental) -----------------------------------------


def changes(*pairs):
    """Records with (updated_on, id), as the API returns them."""
    return [{"id": str(id_), "updated_on": ts} for ts, id_ in pairs]


def changes_matcher(ts, after_id, limit="2"):
    return [
        matchers.query_param_matcher(
            {
                "$limit": limit,
                "$order": "updated_on,id",
                "$where": f"updated_on > '{ts}' OR (updated_on = '{ts}' AND id > {after_id})",
            }
        )
    ]


T1 = "2025-01-01T15:40:23.000"
T2 = "2025-01-02T08:00:00.000"


def test_changes_requests_use_the_incremental_timeout(mocked_api, make_client):
    mocked_api.get(URL, json=[])
    mocked_api.get(URL, json=[])
    client = make_client(timeout=30, incremental_timeout=120)

    client.fetch_page()
    client.fetch_changes_page(T1)

    timeouts = [call.request.req_kwargs["timeout"] for call in mocked_api.calls]
    assert timeouts == [30, 120]


def test_changes_page_orders_by_updated_on_then_id(mocked_api, client):
    mocked_api.get(URL, json=changes((T1, 5)), match=changes_matcher(T1, 0, limit="5000"))

    assert client.fetch_changes_page(T1) == changes((T1, 5))


@pytest.mark.parametrize("bad", ["2025-01-01", "2025-01-01T00:00:00' OR 1=1 --", "yesterday"])
def test_changes_page_rejects_a_bad_timestamp(client, bad):
    with pytest.raises(ValueError, match="Not an API timestamp"):
        client.fetch_changes_page(bad)


def test_iter_changed_pages_continues_from_the_last_change(mocked_api, make_client):
    # Two records share T1, so the id is needed to continue after the right one.
    mocked_api.get(URL, json=changes((T1, 5), (T1, 9)), match=changes_matcher(T1, 0))
    mocked_api.get(URL, json=changes((T2, 3)), match=changes_matcher(T1, 9))

    pages = list(make_client(page_size=2).iter_changed_pages(T1))

    assert pages == [changes((T1, 5), (T1, 9)), changes((T2, 3))]


def test_iter_changed_pages_resumes_from_a_cursor(mocked_api, make_client):
    mocked_api.get(URL, json=[], match=changes_matcher(T2, 3))

    assert list(make_client(page_size=2).iter_changed_pages(T2, after_id=3)) == []


def test_iter_changed_pages_stops_if_paging_does_not_move_forward(mocked_api, make_client):
    # Ends at T1|9, which is not after the T2|3 we asked for.
    mocked_api.get(URL, json=changes((T1, 5), (T1, 9)), match=changes_matcher(T2, 3))

    with pytest.raises(ApiResponseError, match="did not move forward"):
        list(make_client(page_size=2).iter_changed_pages(T2, after_id=3))


def test_iter_changed_pages_needs_updated_on_on_the_last_record(mocked_api, make_client):
    mocked_api.get(URL, json=[{"id": "5", "updated_on": T1}, {"id": "9"}])

    with pytest.raises(ApiResponseError, match="no updated_on or id"):
        list(make_client(page_size=2).iter_changed_pages(T1))


# --- retries ----------------------------------------------------------------


def test_429_then_success_waits_for_retry_after(mocked_api, client, sleeps):
    mocked_api.get(URL, status=429, headers={"Retry-After": "7"})
    mocked_api.get(URL, json=records(1))

    assert client.fetch_page() == records(1)
    assert len(mocked_api.calls) == 2
    assert sleeps == [7.0]


def test_429_without_retry_after_uses_backoff(mocked_api, client, sleeps):
    mocked_api.get(URL, status=429)
    mocked_api.get(URL, json=records(1))

    assert client.fetch_page() == records(1)
    assert sleeps == [1.0]


def test_long_retry_after_is_capped(mocked_api, client, sleeps):
    mocked_api.get(URL, status=429, headers={"Retry-After": "3600"})
    mocked_api.get(URL, json=[])

    client.fetch_page()

    assert sleeps == [60.0]


def test_repeated_429s_raise_rate_limit_error(mocked_api, make_client):
    mocked_api.get(URL, status=429)

    with pytest.raises(ApiRateLimitError):
        make_client(max_attempts=3).fetch_page()

    assert len(mocked_api.calls) == 3


def test_repeated_500s_retry_with_backoff_then_raise(mocked_api, make_client, sleeps):
    mocked_api.get(URL, status=500)

    with pytest.raises(ApiServerError, match="HTTP 500"):
        make_client(max_attempts=4).fetch_page()

    assert len(mocked_api.calls) == 4
    assert sleeps == [1.0, 2.0, 4.0]


@pytest.mark.parametrize(
    "error",
    [requests.ReadTimeout("too slow"), requests.ConnectionError("connection refused")],
)
def test_timeouts_and_connection_errors_are_retried(mocked_api, client, sleeps, error):
    mocked_api.get(URL, body=error)
    mocked_api.get(URL, json=records(1))

    assert client.fetch_page() == records(1)
    assert sleeps == [1.0]


def test_client_error_is_not_retried(mocked_api, client, sleeps):
    mocked_api.get(URL, status=404)

    with pytest.raises(ApiRequestError, match="HTTP 404"):
        client.fetch_page()

    assert len(mocked_api.calls) == 1
    assert sleeps == []


def test_retries_are_logged(mocked_api, client, caplog):
    mocked_api.get(URL, status=503)
    mocked_api.get(URL, json=[])

    with caplog.at_level(logging.WARNING, logger="pipeline.ingestion.client"):
        client.fetch_page()

    assert "attempt 1/5" in caplog.text
    assert "HTTP 503" in caplog.text


# --- logging ----------------------------------------------------------------


def test_each_page_is_logged_with_after_id_and_row_count(mocked_api, client, caplog):
    mocked_api.get(URL, json=records(11, 12, 13))

    with caplog.at_level(logging.INFO, logger="pipeline.ingestion.client"):
        client.fetch_page(after_id=10)

    assert "Fetched page after_id=10 rows=3" in caplog.text
