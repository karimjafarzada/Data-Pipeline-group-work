"""HTTP client for the Chicago Crimes dataset on the Socrata (SODA) API.

Pages are fetched with keyset paging, never with ``$offset`` (see CLAUDE.md, "API notes"):

- ``iter_pages`` (backfill): ordered by ``id``, filtered with ``id > <after_id>``.
- ``iter_changed_pages`` (incremental): ordered by ``updated_on, id``, filtered with
  ``updated_on > <ts> OR (updated_on = <ts> AND id > <after_id>)``. This finds new
  records and records changed since the cursor; ``id`` breaks ties in ``updated_on``.
"""

from __future__ import annotations

import logging
import math
import os
import re
import time
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta
from typing import Any

import requests
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

DATASET_ID = "ijzp-q8t2"
DEFAULT_PAGE_SIZE = 5000
DEFAULT_TIMEOUT_SECONDS = 30.0
# "Changed since" queries sort by updated_on and are slow on the API's side.
DEFAULT_INCREMENTAL_TIMEOUT_SECONDS = 120.0
INCREMENTAL_TIMEOUT_ENV = "API_INCREMENTAL_TIMEOUT_SECONDS"
DEFAULT_MAX_ATTEMPTS = 5
# Backoff waits 1s, 2s, 4s, ... and never more than this.
MAX_BACKOFF_SECONDS = 30.0
# A Retry-After longer than this is cut down, so one header can't stall a run.
MAX_RETRY_AFTER_SECONDS = 60.0
# The API's timestamp format (no timezone), e.g. 2025-01-01T15:40:23.000
API_TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?$")

Record = dict[str, Any]


class ApiError(Exception):
    """Base class for every error raised by this client."""


class ApiConfigError(ApiError):
    """A required setting, such as API_BASE_URL, is missing."""


class ApiRequestError(ApiError):
    """The API rejected the request (a 4xx other than 429). Retrying won't help."""


class ApiResponseError(ApiError):
    """The API answered, but not with the JSON list of records we expect."""


class ApiServerError(ApiError):
    """The API returned a 5xx. Retried; raised once the attempts run out."""


class ApiRateLimitError(ApiError):
    """The API returned 429. Retried; raised once the attempts run out."""

    def __init__(self, message: str, retry_after: float | None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


# Errors that may go away if we wait and try again.
RETRYABLE_ERRORS = (
    requests.Timeout,
    requests.ConnectionError,
    ApiServerError,
    ApiRateLimitError,
)


def build_query_params(
    limit: int,
    after_id: int | str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, str]:
    """Build the SoQL query parameters for one page.

    ``end_date`` is inclusive: the filter is ``date < end_date + 1 day``, so the
    whole last day is included (``between`` would stop at its midnight).
    """
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    conditions = []
    if after_id is not None:
        # int() also stops anything other than a number reaching the query.
        conditions.append(f"id > {int(after_id)}")
    if start_date is not None:
        conditions.append(f"date >= '{start_date.strftime('%Y-%m-%d')}T00:00:00'")
    if end_date is not None:
        day_after_end = end_date + timedelta(days=1)
        conditions.append(f"date < '{day_after_end.strftime('%Y-%m-%d')}T00:00:00'")

    params = {"$limit": str(limit), "$order": "id"}
    if conditions:
        params["$where"] = " AND ".join(conditions)
    return params


def build_changes_query_params(limit: int, updated_after: str, after_id: int | str = 0) -> dict[str, str]:
    """Build the SoQL query parameters for one page of records changed after a cursor.

    ``updated_after`` is a raw API timestamp, used exactly as the API returned it.
    """
    if not API_TIMESTAMP_PATTERN.match(updated_after):
        # Also stops anything other than a timestamp reaching the query.
        raise ValueError(f"Not an API timestamp: {updated_after!r}")
    last_id = int(after_id)
    return {
        "$limit": str(limit),
        "$order": "updated_on,id",
        "$where": (
            f"updated_on > '{updated_after}'"
            f" OR (updated_on = '{updated_after}' AND id > {last_id})"
        ),
    }


class SocrataClient:
    """Fetches pages of crime records from the Socrata API."""

    def __init__(
        self,
        base_url: str,
        app_token: str | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        incremental_timeout: float = DEFAULT_INCREMENTAL_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not base_url:
            raise ApiConfigError("base_url is required")
        if page_size < 1:
            raise ValueError("page_size must be at least 1")
        if timeout <= 0 or incremental_timeout <= 0:
            raise ValueError("timeouts must be more than 0 seconds")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        self.url = f"{base_url.rstrip('/')}/resource/{DATASET_ID}.json"
        # The dataset's metadata, including its columns (used by check_api_schema()).
        self.metadata_url = f"{base_url.rstrip('/')}/api/views/{DATASET_ID}.json"
        self.page_size = page_size
        self.timeout = timeout  # for backfill (id-ordered) requests
        self.incremental_timeout = incremental_timeout  # for "changed since" requests
        self.max_attempts = max_attempts
        self.session = session or requests.Session()
        # The token goes in a header, never in the URL, so it can't end up in logs.
        self._headers = {"X-App-Token": app_token} if app_token else {}
        # Tests pass a fake sleep so retries don't really wait.
        self._sleep = sleep
        self._backoff = wait_exponential(multiplier=1, min=1, max=MAX_BACKOFF_SECONDS)

    @classmethod
    def from_env(cls, **kwargs: Any) -> SocrataClient:
        """Build a client from API_BASE_URL and the optional SOCRATA_APP_TOKEN and
        API_INCREMENTAL_TIMEOUT_SECONDS. Keyword arguments override the environment."""
        base_url = os.environ.get("API_BASE_URL")
        if not base_url:
            raise ApiConfigError("API_BASE_URL is not set. Add it to your .env file.")
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or None
        raw_timeout = os.environ.get(INCREMENTAL_TIMEOUT_ENV)
        if raw_timeout and "incremental_timeout" not in kwargs:
            kwargs["incremental_timeout"] = _parse_seconds(INCREMENTAL_TIMEOUT_ENV, raw_timeout)
        return cls(base_url, app_token=app_token, **kwargs)

    def fetch_page(
        self,
        after_id: int | str | None = None,
        limit: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[Record]:
        """Fetch one page of records with ``id > after_id``, ordered by ``id``."""
        if limit is None:
            limit = self.page_size
        params = build_query_params(limit, after_id, start_date, end_date)
        rows = self._fetch(params, self.timeout)
        logger.info("Fetched page after_id=%s rows=%d", after_id, len(rows))
        return rows

    def fetch_changes_page(
        self, updated_after: str, after_id: int | str = 0, limit: int | None = None
    ) -> list[Record]:
        """Fetch one page of records changed after (updated_after, after_id)."""
        if limit is None:
            limit = self.page_size
        params = build_changes_query_params(limit, updated_after, after_id)
        rows = self._fetch(params, self.incremental_timeout)
        logger.info(
            "Fetched changes page after=%s|%s rows=%d", updated_after, after_id, len(rows)
        )
        return rows

    def fetch_columns(self) -> list[Record]:
        """The dataset's columns from the metadata endpoint, with the same retries as pages.

        Each column is a dict with at least ``fieldName`` and ``dataTypeName``.
        """
        response = self._send(self.metadata_url, {}, self.timeout)
        columns = _parse_columns(response)
        logger.info("Fetched dataset metadata: %d columns", len(columns))
        return columns

    def _fetch(self, params: dict[str, str], timeout: float) -> list[Record]:
        """Send the request with retries and return the parsed list of records."""
        return _parse_records(self._send(self.url, params, timeout))

    def _send(self, url: str, params: dict[str, str], timeout: float) -> requests.Response:
        """Send a GET request to url, retrying the errors that may go away."""
        retrying = Retrying(
            retry=retry_if_exception_type(RETRYABLE_ERRORS),
            stop=stop_after_attempt(self.max_attempts),
            wait=self._wait_seconds,
            sleep=self._sleep,
            before_sleep=self._log_retry,
            # Raise the last real error (e.g. ApiServerError), not tenacity's RetryError.
            reraise=True,
        )
        return retrying(self._get, url, params, timeout)

    def iter_pages(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        after_id: int | str | None = None,
        limit: int | None = None,
    ) -> Iterator[list[Record]]:
        """Yield pages of records in ``id`` order until a page comes back short.

        Pass the last ``after_id`` you saved to resume. Empty pages are not yielded.
        """
        if limit is None:
            limit = self.page_size

        while True:
            page = self.fetch_page(after_id, limit, start_date, end_date)
            if page:
                yield page
            if len(page) < limit:
                return
            after_id = _next_after_id(page, after_id)

    def iter_changed_pages(
        self, updated_after: str, after_id: int | str = 0, limit: int | None = None
    ) -> Iterator[list[Record]]:
        """Yield pages of records changed after (updated_after, after_id), oldest change first.

        Stops when a page comes back short. Empty pages are not yielded.
        """
        if limit is None:
            limit = self.page_size

        while True:
            page = self.fetch_changes_page(updated_after, after_id, limit)
            if page:
                yield page
            if len(page) < limit:
                return
            updated_after, after_id = _next_change_cursor(page, (updated_after, after_id))

    def _get(self, url: str, params: dict[str, str], timeout: float) -> requests.Response:
        """Send one request and turn a bad status code into the matching error."""
        response = self.session.get(url, params=params, headers=self._headers, timeout=timeout)
        status = response.status_code
        if status == 429:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            raise ApiRateLimitError("HTTP 429 Too Many Requests", retry_after)
        if status >= 500:
            raise ApiServerError(f"HTTP {status} from API")
        if status >= 400:
            raise ApiRequestError(f"HTTP {status} from API: {response.text[:200]}")
        return response

    def _wait_seconds(self, retry_state: RetryCallState) -> float:
        """How long to wait before the next attempt."""
        error = retry_state.outcome.exception()
        if isinstance(error, ApiRateLimitError) and error.retry_after is not None:
            return min(error.retry_after, MAX_RETRY_AFTER_SECONDS)
        return self._backoff(retry_state)

    def _log_retry(self, retry_state: RetryCallState) -> None:
        logger.warning(
            "API request failed (attempt %d/%d): %s. Retrying in %.1fs",
            retry_state.attempt_number,
            self.max_attempts,
            retry_state.outcome.exception(),
            retry_state.next_action.sleep,
        )


def _parse_seconds(name: str, value: str) -> float:
    """Read a positive number of seconds from an environment variable."""
    try:
        seconds = float(value)
    except ValueError:
        seconds = math.nan
    if not math.isfinite(seconds) or seconds <= 0:
        raise ApiConfigError(f"{name} must be a positive number of seconds, got {value!r}")
    return seconds


def _parse_retry_after(value: str | None) -> float | None:
    """Read a Retry-After header given in seconds.

    Returns None when the header is missing or not a usable number of seconds
    (for example the HTTP-date form); the caller then falls back to backoff.
    """
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def page_max_id(page: list[Record]) -> int:
    """The highest id on a page (records without an id are ignored)."""
    ids = [int(record["id"]) for record in page if "id" in record]
    if not ids:
        raise ApiResponseError("The page has no record ids, so paging can't continue")
    return max(ids)


def page_last_change(page: list[Record]) -> tuple[str, int]:
    """(updated_on, id) of the last record on a page ordered by updated_on, id."""
    last = page[-1]
    if "updated_on" not in last or "id" not in last:
        raise ApiResponseError(
            "The last record on the page has no updated_on or id, so paging can't continue"
        )
    return last["updated_on"], int(last["id"])


def _next_after_id(page: list[Record], previous: int | str | None) -> int:
    """The highest id on the page: where the next page starts."""
    next_id = page_max_id(page)
    # The API should only return ids above `previous`; stop rather than loop forever.
    if previous is not None and next_id <= int(previous):
        raise ApiResponseError(
            f"Paging did not move forward (after_id {previous}, page max id {next_id})"
        )
    return next_id


def _next_change_cursor(page: list[Record], previous: tuple[str, int | str]) -> tuple[str, int]:
    """(updated_on, id) of the page's last record: where the next page starts."""
    updated_on, last_id = page_last_change(page)
    # Compare as datetimes, because '...:23' and '...:23.000' are the same moment.
    new_position = (datetime.fromisoformat(updated_on), last_id)
    old_position = (datetime.fromisoformat(previous[0]), int(previous[1]))
    if new_position <= old_position:
        raise ApiResponseError(
            f"Paging did not move forward (after {previous[0]}|{previous[1]},"
            f" page ends at {updated_on}|{last_id})"
        )
    return updated_on, last_id


def _parse_records(response: requests.Response) -> list[Record]:
    try:
        data = response.json()
    except ValueError as exc:
        raise ApiResponseError("API response is not valid JSON") from exc
    if not isinstance(data, list):
        raise ApiResponseError(
            f"Expected a JSON list of records, got {type(data).__name__}"
        )
    return data


def _parse_columns(response: requests.Response) -> list[Record]:
    """The ``columns`` list of a metadata response; each needs fieldName and dataTypeName."""
    try:
        data = response.json()
    except ValueError as exc:
        raise ApiResponseError("Metadata response is not valid JSON") from exc
    columns = data.get("columns") if isinstance(data, dict) else None
    if not isinstance(columns, list) or not all(
        isinstance(c, dict) and isinstance(c.get("fieldName"), str)
        and isinstance(c.get("dataTypeName"), str)
        for c in columns
    ):
        raise ApiResponseError(
            "Expected metadata with a 'columns' list of fieldName/dataTypeName entries"
        )
    return columns
