"""
Unit tests for tap_pipedrive.tap module.

Covers: do_discover, iterate_response, validate_response, rate_throttling,
        get_selected_streams, raise_for_error (5xx catch-all), do_paginate.
"""
import time
import unittest
from unittest.mock import MagicMock, patch, call

import requests

from tap_pipedrive.tap import PipedriveTap, raise_for_error
from tap_pipedrive.exceptions import (
    PipedriveBadRequestError,
    PipedriveUnauthorizedError,
    PipedrivePaymentRequiredError,
    PipedriveForbiddenError,
    PipedriveNotFoundError,
    PipedriveGoneError,
    PipedriveUnsupportedMediaError,
    PipedriveUnprocessableEntityError,
    PipedriveTooManyRequestsError,
    PipedriveTooManyRequestsInSecondError,
    PipedriveInternalServiceError,
    PipedriveNotImplementedError,
    PipedriveServiceUnavailableError,
    Pipedrive5xxError,
    PipedriveError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**kwargs):
    base = {"api_token": "test_token", "start_date": "2024-01-01T00:00:00Z"}
    base.update(kwargs)
    return base


def _make_response(status_code, json_body=None, headers=None, raise_exc=None):
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.url = "https://api.pipedrive.com/api/v2/test"
    resp.text = str(json_body)
    if json_body is not None:
        resp.json.return_value = json_body
    else:
        resp.json.side_effect = ValueError("no body")
    if raise_exc:
        resp.raise_for_status.side_effect = raise_exc
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_http_error(status_code, json_body=None, headers=None):
    """Build an HTTPError with an attached response mock."""
    resp = _make_response(status_code, json_body=json_body, headers=headers)
    err = requests.HTTPError(response=resp)
    err.response = resp
    resp.raise_for_status.side_effect = err
    return resp


def _make_tap():
    return PipedriveTap(_make_config(), {})


# ---------------------------------------------------------------------------
# TestIterateResponse
# ---------------------------------------------------------------------------

class TestIterateResponse(unittest.TestCase):
    """Verify iterate_response correctly handles null and non-null payloads."""

    def test_null_data_returns_empty_list(self):
        """iterate_response yields [] when payload['data'] is None."""
        tap = _make_tap()
        resp = _make_response(200, json_body={"data": None})
        result = tap.iterate_response(resp)
        self.assertEqual(result, [])

    def test_non_null_data_returns_records(self):
        """iterate_response yields the data list when payload contains records."""
        tap = _make_tap()
        records = [{"id": 1}, {"id": 2}]
        resp = _make_response(200, json_body={"data": records})
        result = tap.iterate_response(resp)
        self.assertEqual(result, records)


# ---------------------------------------------------------------------------
# TestValidateResponse
# ---------------------------------------------------------------------------

class TestValidateResponse(unittest.TestCase):
    """Verify validate_response returns True for valid Singer payloads."""

    def test_valid_success_payload_returns_true(self):
        """validate_response returns True when 'success' is True and 'data' is present."""
        tap = _make_tap()
        resp = _make_response(200, json_body={"success": True, "data": [{"id": 1}]})
        self.assertTrue(tap.validate_response(resp))

    def test_success_false_returns_none(self):
        """validate_response returns None when 'success' is False."""
        tap = _make_tap()
        resp = _make_response(200, json_body={"success": False, "data": []})
        self.assertIsNone(tap.validate_response(resp))

    def test_missing_data_key_returns_none(self):
        """validate_response returns None when 'data' key is absent."""
        tap = _make_tap()
        resp = _make_response(200, json_body={"success": True})
        self.assertIsNone(tap.validate_response(resp))


# ---------------------------------------------------------------------------
# TestRateThrottling
# ---------------------------------------------------------------------------

class TestRateThrottling(unittest.TestCase):
    """Verify rate_throttling sleeps correctly when rate-limit headers are present."""

    @patch("tap_pipedrive.tap.time.sleep")
    def test_sleeps_when_remaining_is_zero(self, mock_sleep):
        """rate_throttling sleeps for X-RateLimit-Reset seconds when remaining < 1."""
        tap = _make_tap()
        resp = _make_response(
            200,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "5"},
        )
        tap.rate_throttling(resp)
        mock_sleep.assert_called_once_with(5)

    @patch("tap_pipedrive.tap.time.sleep")
    def test_no_sleep_when_remaining_above_zero(self, mock_sleep):
        """rate_throttling does not sleep when X-RateLimit-Remaining >= 1."""
        tap = _make_tap()
        resp = _make_response(
            200,
            headers={"X-RateLimit-Remaining": "10", "X-RateLimit-Reset": "5"},
        )
        tap.rate_throttling(resp)
        mock_sleep.assert_not_called()

    @patch("tap_pipedrive.tap.time.sleep")
    def test_no_sleep_when_headers_absent(self, mock_sleep):
        """rate_throttling does not sleep when rate-limit headers are missing."""
        tap = _make_tap()
        resp = _make_response(200, headers={})
        tap.rate_throttling(resp)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# TestGetSelectedStreams
# ---------------------------------------------------------------------------

class TestGetSelectedStreams(unittest.TestCase):
    """Verify get_selected_streams filters according to catalog metadata."""

    def _make_catalog_stream(self, tap_stream_id, selected):
        stream = MagicMock()
        stream.tap_stream_id = tap_stream_id
        stream.metadata = [{"breadcrumb": [], "metadata": {"selected": selected}}]
        return stream

    def test_returns_selected_streams(self):
        """get_selected_streams returns only streams marked as selected=True."""
        tap = _make_tap()
        catalog = MagicMock()
        catalog.streams = [
            self._make_catalog_stream("deals", True),
            self._make_catalog_stream("activities", False),
            self._make_catalog_stream("users", True),
        ]
        result = tap.get_selected_streams(catalog)
        self.assertIn("deals", result)
        self.assertIn("users", result)
        self.assertNotIn("activities", result)

    def test_returns_empty_when_none_selected(self):
        """get_selected_streams returns an empty list when no streams are selected."""
        tap = _make_tap()
        catalog = MagicMock()
        catalog.streams = [
            self._make_catalog_stream("deals", False),
        ]
        result = tap.get_selected_streams(catalog)
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# TestRaiseForError
# ---------------------------------------------------------------------------

class TestRaiseForError(unittest.TestCase):
    """Verify raise_for_error maps HTTP status codes to the correct exceptions."""

    def _call(self, status_code, json_body=None, headers=None):
        resp = _make_http_error(status_code, json_body=json_body, headers=headers)
        raise_for_error(resp)

    def test_400_raises_bad_request(self):
        """raise_for_error raises PipedriveBadRequestError on HTTP 400."""
        with self.assertRaises(PipedriveBadRequestError):
            self._call(400, json_body={"error": "bad input"})

    def test_401_raises_unauthorized(self):
        """raise_for_error raises PipedriveUnauthorizedError on HTTP 401."""
        with self.assertRaises(PipedriveUnauthorizedError):
            self._call(401, json_body={})

    def test_402_raises_payment_required(self):
        """raise_for_error raises PipedrivePaymentRequiredError on HTTP 402."""
        with self.assertRaises(PipedrivePaymentRequiredError):
            self._call(402, json_body={})

    def test_403_raises_forbidden(self):
        """raise_for_error raises PipedriveForbiddenError on HTTP 403."""
        with self.assertRaises(PipedriveForbiddenError):
            self._call(403, json_body={})

    def test_404_raises_not_found(self):
        """raise_for_error raises PipedriveNotFoundError on HTTP 404."""
        with self.assertRaises(PipedriveNotFoundError):
            self._call(404, json_body={})

    def test_410_raises_gone(self):
        """raise_for_error raises PipedriveGoneError on HTTP 410."""
        with self.assertRaises(PipedriveGoneError):
            self._call(410, json_body={})

    def test_415_raises_unsupported_media(self):
        """raise_for_error raises PipedriveUnsupportedMediaError on HTTP 415."""
        with self.assertRaises(PipedriveUnsupportedMediaError):
            self._call(415, json_body={})

    def test_422_raises_unprocessable_entity(self):
        """raise_for_error raises PipedriveUnprocessableEntityError on HTTP 422."""
        with self.assertRaises(PipedriveUnprocessableEntityError):
            self._call(422, json_body={})

    def test_500_raises_internal_service_error(self):
        """raise_for_error raises PipedriveInternalServiceError on HTTP 500."""
        with self.assertRaises(PipedriveInternalServiceError):
            self._call(500, json_body={})

    def test_501_raises_not_implemented(self):
        """raise_for_error raises PipedriveNotImplementedError on HTTP 501."""
        with self.assertRaises(PipedriveNotImplementedError):
            self._call(501, json_body={})

    def test_503_raises_service_unavailable(self):
        """raise_for_error raises PipedriveServiceUnavailableError on HTTP 503."""
        with self.assertRaises(PipedriveServiceUnavailableError):
            self._call(503, json_body={})

    def test_5xx_catchall_raises_pipedrive5xx_error(self):
        """raise_for_error raises Pipedrive5xxError for unlisted 5xx codes (e.g. 524)."""
        with self.assertRaises(Pipedrive5xxError):
            self._call(524, json_body={})

    def test_error_message_includes_status_code(self):
        """raise_for_error includes the HTTP status code in the exception message."""
        with self.assertRaises(PipedriveNotFoundError) as ctx:
            self._call(404, json_body={})
        self.assertIn("404", str(ctx.exception))

    def test_429_rate_limit_per_second_raises_too_many_requests_in_second(self):
        """raise_for_error raises PipedriveTooManyRequestsInSecondError when X-RateLimit-Remaining is 0."""
        with self.assertRaises(PipedriveTooManyRequestsInSecondError):
            self._call(
                429,
                json_body={},
                headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "2"},
            )


# ---------------------------------------------------------------------------
# TestDiscover
# ---------------------------------------------------------------------------

_MOCK_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": ["null", "integer"]},
        "update_time": {"type": ["null", "string"]},
        "log_time": {"type": ["null", "string"]},
        "add_time": {"type": ["null", "string"]},
        "modified": {"type": ["null", "string"]},
    },
}


class TestDiscover(unittest.TestCase):
    """Verify do_discover builds a valid Singer Catalog."""

    @patch("tap_pipedrive.stream.DynamicSchemaStream.get_schema", return_value=_MOCK_SCHEMA)
    @patch("tap_pipedrive.stream.PipedriveStream.get_schema", return_value=_MOCK_SCHEMA)
    def test_discover_returns_catalog_with_all_streams(self, mock_base, mock_dyn):
        """do_discover returns a Catalog containing an entry for every configured stream."""
        tap = _make_tap()
        catalog = tap.do_discover()
        stream_ids = [s.tap_stream_id for s in catalog.streams]
        self.assertIn("deals", stream_ids)
        self.assertIn("users", stream_ids)
        self.assertIn("activities", stream_ids)

    @patch("tap_pipedrive.stream.DynamicSchemaStream.get_schema", return_value=_MOCK_SCHEMA)
    @patch("tap_pipedrive.stream.PipedriveStream.get_schema", return_value=_MOCK_SCHEMA)
    def test_discover_sets_key_properties(self, mock_base, mock_dyn):
        """do_discover attaches the correct key_properties to each catalog entry."""
        tap = _make_tap()
        catalog = tap.do_discover()
        deals_entry = next(s for s in catalog.streams if s.tap_stream_id == "deals")
        self.assertEqual(deals_entry.key_properties, ["id"])

    @patch("tap_pipedrive.stream.DynamicSchemaStream.get_schema", return_value=_MOCK_SCHEMA)
    @patch("tap_pipedrive.stream.PipedriveStream.get_schema", return_value=_MOCK_SCHEMA)
    def test_discover_incremental_stream_has_replication_key_in_metadata(self, mock_base, mock_dyn):
        """do_discover marks the replication key as automatic for incremental streams."""
        from singer import metadata
        tap = _make_tap()
        catalog = tap.do_discover()
        deals_entry = next(s for s in catalog.streams if s.tap_stream_id == "deals")
        mdata = metadata.to_map(deals_entry.metadata)
        from tap_pipedrive.streams.deals import DealsStream
        deals_stream = DealsStream()
        if deals_stream.state_field:
            self.assertEqual(
                mdata.get(("properties", deals_stream.state_field), {}).get("inclusion"),
                "automatic",
            )


# ---------------------------------------------------------------------------
# TestDoPaginate
# ---------------------------------------------------------------------------

class TestDoPaginate(unittest.TestCase):
    """Verify do_paginate writes schema, records, and state correctly."""

    def _build_stream_and_catalog(self, schema_name="currencies"):
        """Build a minimal stream + catalog metadata mock for do_paginate."""
        from tap_pipedrive.streams.currencies import CurrenciesStream
        stream = CurrenciesStream()
        stream.tap = None  # will be set by tap

        catalog_stream = MagicMock()
        catalog_stream.metadata = [{"breadcrumb": [], "metadata": {}}]
        catalog = MagicMock()
        catalog.get_stream.return_value = catalog_stream
        return stream, catalog

    @patch("singer.write_state")
    @patch("singer.write_record")
    @patch("singer.write_bookmark", side_effect=lambda state, *a, **kw: state)
    @patch("tap_pipedrive.tap.PipedriveTap.execute_stream_request")
    def test_do_paginate_writes_records(
        self, mock_exec, mock_bookmark, mock_write_record, mock_write_state
    ):
        """do_paginate calls singer.write_record for every row returned by the API."""
        tap = _make_tap()
        stream, catalog = self._build_stream_and_catalog()
        stream.tap = tap

        records = [{"id": 1, "name": "USD"}, {"id": 2, "name": "EUR"}]
        page1 = _make_response(
            200,
            json_body={"success": True, "data": records, "additional_data": {}},
            headers={"X-RateLimit-Remaining": "10"},
        )
        # First call returns data; second call: stream.has_data() returns False after paginate
        mock_exec.return_value = page1

        stream.more_items_in_collection = True
        stream.initial_state = "2024-01-01T00:00:00Z"

        tap.do_paginate(stream, {})

        self.assertEqual(mock_write_record.call_count, len(records))

    @patch("singer.write_state")
    @patch("singer.write_record")
    @patch("singer.write_bookmark", side_effect=lambda state, *a, **kw: state)
    @patch("tap_pipedrive.tap.PipedriveTap.execute_stream_request")
    def test_do_paginate_with_empty_data_writes_no_records(
        self, mock_exec, mock_bookmark, mock_write_record, mock_write_state
    ):
        """do_paginate writes no records when the API returns an empty data list."""
        tap = _make_tap()
        stream, catalog = self._build_stream_and_catalog()
        stream.tap = tap

        page = _make_response(
            200,
            json_body={"success": True, "data": [], "additional_data": {}},
            headers={"X-RateLimit-Remaining": "10"},
        )
        mock_exec.return_value = page
        stream.more_items_in_collection = True
        stream.initial_state = "2024-01-01T00:00:00Z"

        tap.do_paginate(stream, {})

        mock_write_record.assert_not_called()


if __name__ == "__main__":
    unittest.main()
