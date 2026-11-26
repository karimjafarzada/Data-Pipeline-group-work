"""HTTP client for the Chicago Crimes dataset on the Socrata (SODA) API.

Pages are fetched with keyset paging: ordered by ``id`` and filtered with
``id > <after_id>``, never with ``$offset`` (see CLAUDE.md, "API notes").
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections.abc import Callable
from datetime import date, timedelta
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
DEFAULT_MAX_ATTEMPTS = 5
# Backoff waits 1s, 2s, 4s, ... and never more than this.
MAX_BACKOFF_SECONDS = 30.0
# A Retry-After longer than this is cut down, so one header can't stall a run.
MAX_RETRY_AFTER_SECONDS = 60.0

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


class SocrataClient:
    """Fetches pages of crime records from the Socrata API."""

    def __init__(
        self,
        base_url: str,
        app_token: str | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not base_url:
            raise ApiConfigError("base_url is required")
        if page_size < 1:
            raise ValueError("page_size must be at least 1")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        self.url = f"{base_url.rstrip('/')}/resource/{DATASET_ID}.json"
        self.page_size = page_size
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.session = session or requests.Session()
        # The token goes in a header, never in the URL, so it can't end up in logs.
        self._headers = {"X-App-Token": app_token} if app_token else {}
        # Tests pass a fake sleep so retries don't really wait.
        self._sleep = sleep
        self._backoff = wait_exponential(multiplier=1, min=1, max=MAX_BACKOFF_SECONDS)

    @classmethod
    def from_env(cls, **kwargs: Any) -> SocrataClient:
        """Build a client from API_BASE_URL and the optional SOCRATA_APP_TOKEN."""
        base_url = os.environ.get("API_BASE_URL")
        if not base_url:
            raise ApiConfigError("API_BASE_URL is not set. Add it to your .env file.")
        app_token = os.environ.get("SOCRATA_APP_TOKEN") or None
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

        retrying = Retrying(
            retry=retry_if_exception_type(RETRYABLE_ERRORS),
            stop=stop_after_attempt(self.max_attempts),
            wait=self._wait_seconds,
            sleep=self._sleep,
            before_sleep=self._log_retry,
            # Raise the last real error (e.g. ApiServerError), not tenacity's RetryError.
            reraise=True,
        )
        response = retrying(self._get, params)

        rows = _parse_records(response)
        logger.info("Fetched page after_id=%s rows=%d", after_id, len(rows))
        return rows

    def _get(self, params: dict[str, str]) -> requests.Response:
        """Send one request and turn a bad status code into the matching error."""
        response = self.session.get(
            self.url, params=params, headers=self._headers, timeout=self.timeout
        )
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
