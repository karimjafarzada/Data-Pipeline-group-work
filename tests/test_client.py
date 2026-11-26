"""Tests for the Socrata API client. The API is always mocked with `responses`."""

import logging
from datetime import date

import pytest
import responses
from responses import matchers

from pipeline.ingestion.client import (
    ApiConfigError,
    ApiRequestError,
    ApiResponseError,
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
def client():
    return SocrataClient(BASE_URL)


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


def test_page_size_is_configurable(mocked_api):
    mocked_api.get(
        URL,
        json=[],
        match=[matchers.query_param_matcher({"$limit": "250", "$order": "id"})],
    )

    SocrataClient(BASE_URL, page_size=250).fetch_page()


def test_start_date_after_end_date_is_rejected(client):
    with pytest.raises(ValueError, match="after end_date"):
        client.fetch_page(start_date=date(2024, 2, 1), end_date=date(2024, 1, 1))


# --- app token --------------------------------------------------------------


def test_app_token_is_sent_as_header(mocked_api):
    mocked_api.get(
        URL, json=[], match=[matchers.header_matcher({"X-App-Token": "secret-token"})]
    )

    SocrataClient(BASE_URL, app_token="secret-token").fetch_page()


def test_no_app_token_header_without_token(mocked_api, client):
    mocked_api.get(URL, json=[])

    client.fetch_page()

    assert "X-App-Token" not in mocked_api.calls[0].request.headers


# --- configuration from the environment -------------------------------------


def test_from_env_reads_base_url_and_token(mocked_api, monkeypatch):
    monkeypatch.setenv("API_BASE_URL", BASE_URL + "/")
    monkeypatch.setenv("SOCRATA_APP_TOKEN", "env-token")
    mocked_api.get(
        URL, json=[], match=[matchers.header_matcher({"X-App-Token": "env-token"})]
    )

    client = SocrataClient.from_env()
    client.fetch_page()

    assert client.url == URL


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


# --- logging ----------------------------------------------------------------


def test_each_page_is_logged_with_after_id_and_row_count(mocked_api, client, caplog):
    mocked_api.get(URL, json=records(11, 12, 13))

    with caplog.at_level(logging.INFO, logger="pipeline.ingestion.client"):
        client.fetch_page(after_id=10)

    assert "Fetched page after_id=10 rows=3" in caplog.text
