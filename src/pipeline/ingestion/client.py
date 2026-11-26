"""HTTP client for the Chicago Crimes dataset on the Socrata (SODA) API.

Pages are fetched with keyset paging: ordered by ``id`` and filtered with
``id > <after_id>``, never with ``$offset`` (see CLAUDE.md, "API notes").
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

DATASET_ID = "ijzp-q8t2"
DEFAULT_PAGE_SIZE = 5000
DEFAULT_TIMEOUT_SECONDS = 30.0

Record = dict[str, Any]


class ApiError(Exception):
    """Base class for every error raised by this client."""


class ApiConfigError(ApiError):
    """A required setting, such as API_BASE_URL, is missing."""


class ApiRequestError(ApiError):
    """The API rejected the request (a 4xx other than 429). Retrying won't help."""


class ApiResponseError(ApiError):
    """The API answered, but not with the JSON list of records we expect."""


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
        session: requests.Session | None = None,
    ) -> None:
        if not base_url:
            raise ApiConfigError("base_url is required")
        if page_size < 1:
            raise ValueError("page_size must be at least 1")

        self.url = f"{base_url.rstrip('/')}/resource/{DATASET_ID}.json"
        self.page_size = page_size
        self.timeout = timeout
        self.session = session or requests.Session()
        # The token goes in a header, never in the URL, so it can't end up in logs.
        self._headers = {"X-App-Token": app_token} if app_token else {}

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

        response = self.session.get(
            self.url, params=params, headers=self._headers, timeout=self.timeout
        )
        if response.status_code >= 400:
            raise ApiRequestError(
                f"HTTP {response.status_code} from API: {response.text[:200]}"
            )

        rows = _parse_records(response)
        logger.info("Fetched page after_id=%s rows=%d", after_id, len(rows))
        return rows


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
