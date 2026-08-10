import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import ConnectionError
import requests
import simplejson

from tap_pipedrive.tap import PipedriveError, PipedriveNull200Error, PipedriveTap, raise_for_error


class _DummyStream:
    def __init__(self, schema, state_field=None, id_list=False):
        self.schema = schema
        self.state_field = state_field
        self.id_list = id_list
        self.endpoint = schema
        self.api_version = "v1"
        self.start = 0
        self.limit = 100
        self.more_items_in_collection = True
        self.earliest_state = "2024-01-01T00:00:00Z"
        self.initial_state = "2024-01-01T00:00:00Z"
        self.more_ids_to_get = False
        self.next_start = 10
        self.stream_start = "2024-01-01T03:00:00+00:00"
        self.these_deals = [101, 102]

    def set_initial_state(self, state, start_date):
        self.initial_state = start_date
        self.earliest_state = start_date

    def write_schema(self):
        return None

    def get_deal_ids(self, _tap):
        for deal_id in self.these_deals:
            yield deal_id

    def update_endpoint(self, deal_id):
        self.endpoint = f"deals/{deal_id}/flow"

    def update_request_params(self, params):
        params["custom"] = "ok"
        return params

    def has_data(self):
        current = self.more_items_in_collection
        self.more_items_in_collection = False
        return current

    def paginate(self, _response):
        self.more_items_in_collection = False

    def process_row(self, row):
        return row

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "integer"]},
                "custom_fields": {"type": ["null", "object"]},
            },
        }

    def write_record(self, _row):
        return True

    def update_state(self, row):
        self.earliest_state = row.get("update_time", self.earliest_state)


def _catalog_with_selected(*stream_ids):
    catalog = MagicMock()
    catalog.streams = []
    metadata_list = [{"breadcrumb": [], "metadata": {"selected": True}}]
    for stream_id in stream_ids:
        stream = MagicMock()
        stream.tap_stream_id = stream_id
        stream.metadata = metadata_list
        catalog.streams.append(stream)
    catalog.get_stream.return_value = MagicMock(metadata=metadata_list)
    return catalog


class TestTapSyncCoverage(unittest.TestCase):
    @patch("tap_pipedrive.tap.singer.write_state")
    @patch("tap_pipedrive.tap.singer.write_bookmark", side_effect=lambda state, *args, **kwargs: state)
    @patch("tap_pipedrive.tap.set_currently_syncing")
    @patch("tap_pipedrive.tap.PipedriveTap.do_paginate")
    def test_do_sync_handles_resume_and_id_list_paths(self, mock_paginate, _set_currently_syncing, _write_bookmark, _write_state):
        state = {"bookmarks": {}, "currently_syncing": "dealflow"}
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, state)

        skipped = _DummyStream("deals", state_field="update_time", id_list=False)
        iter_stream = _DummyStream("dealflow", state_field="log_time", id_list=True)
        iter_stream.more_ids_to_get = True
        tap.streams = [skipped, iter_stream]

        catalog = _catalog_with_selected("deals", "dealflow")

        tap.do_sync(catalog)

        self.assertGreaterEqual(mock_paginate.call_count, 2)
        self.assertNotIn("currently_syncing", tap.state)

    @patch("tap_pipedrive.tap.singer.write_state")
    @patch("tap_pipedrive.tap.PipedriveTap.do_paginate")
    def test_do_sync_clears_invalid_currently_syncing(self, mock_paginate, _write_state):
        state = {"bookmarks": {}, "currently_syncing": "non_selected"}
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, state)
        stream = _DummyStream("deals", state_field="update_time", id_list=False)
        tap.streams = [stream]
        catalog = _catalog_with_selected("deals")

        tap.do_sync(catalog)

        self.assertNotIn("currently_syncing", tap.state)
        mock_paginate.assert_called_once()

    @patch("tap_pipedrive.tap.singer.write_state")
    @patch("tap_pipedrive.tap.PipedriveTap.do_paginate")
    def test_do_sync_skips_unselected_stream_and_hits_keyerror_cleanup(self, mock_paginate, _write_state):
        state = {"bookmarks": {}}
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, state)
        stream = _DummyStream("deals", state_field=None, id_list=False)
        tap.streams = [stream]

        catalog = MagicMock()
        selected_stream = MagicMock()
        selected_stream.tap_stream_id = "other_stream"
        selected_stream.metadata = [{"breadcrumb": [], "metadata": {"selected": True}}]
        catalog.streams = [selected_stream]

        tap.do_sync(catalog)

        mock_paginate.assert_not_called()

    def test_execute_stream_request_builds_params(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {})
        stream = _DummyStream("deals")
        with patch.object(tap, "execute_request", return_value="ok") as mock_exec:
            result = tap.execute_stream_request(stream)
        self.assertEqual("ok", result)
        args = mock_exec.call_args.args
        kwargs = mock_exec.call_args.kwargs
        self.assertEqual("deals", args[0])
        self.assertEqual("v1", args[1])
        self.assertEqual(100, kwargs["params"]["limit"])
        self.assertEqual("ok", kwargs["params"]["custom"])

    @patch("tap_pipedrive.tap.requests.get")
    def test_execute_request_uses_params_and_raises_for_flow_null_body(self, mock_get):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z", "request_timeout": "15"}, {})
        response = requests.Response()
        response.status_code = 200
        response.url = "https://api.pipedrive.com/v1/deals/1/flow"
        response._content = b"null"
        mock_get.return_value = response

        with self.assertRaises(PipedriveNull200Error):
            tap.execute_request("deals/1/flow", "v1", params={"a": 1})

        called_kwargs = mock_get.call_args.kwargs
        self.assertEqual(15.0, called_kwargs["timeout"])
        self.assertEqual(1, called_kwargs["params"]["a"])

    @patch("tap_pipedrive.tap.singer.write_state")
    @patch("tap_pipedrive.tap.singer.write_record")
    @patch("tap_pipedrive.tap.singer.write_bookmark", side_effect=lambda state, *args, **kwargs: state)
    def test_do_paginate_handles_empty_row_and_custom_fields_and_final_max_bookmark(self, _write_bookmark, mock_write_record, _write_state):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {"bookmarks": {}})
        stream = _DummyStream("notes", state_field="update_time", id_list=False)
        stream.max_replication_key_value = "2024-03-01T00:00:00Z"
        stream.earliest_state = "2024-01-01T00:00:00Z"

        response = MagicMock()
        response.status_code = 200
        response.headers = {"X-RateLimit-Remaining": "2", "X-RateLimit-Reset": "1"}
        response.json.return_value = {
            "success": True,
            "data": [
                {},
                {"id": 1, "custom_fields": {"cf": "v"}, "update_time": "2024-02-01T00:00:00Z"},
            ],
        }

        with patch.object(tap, "execute_stream_request", return_value=response):
            tap.do_paginate(stream, {})

        mock_write_record.assert_called_once()
        self.assertEqual("2024-03-01T00:00:00Z", stream.earliest_state)

    def test_do_paginate_reraises_connection_error(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {})
        stream = _DummyStream("notes", state_field="update_time", id_list=False)
        with patch.object(tap, "execute_stream_request", side_effect=ConnectionError("boom")):
            with self.assertRaises(ConnectionError):
                tap.do_paginate(stream, {})

    def test_validate_response_handles_json_decode_error(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {})
        response = MagicMock()
        response.json.side_effect = simplejson.scanner.JSONDecodeError("msg", "doc", 0)
        self.assertIsNone(tap.validate_response(response))

    def test_raise_for_error_value_error_falls_back_to_pipedrive_error(self):
        response = MagicMock()
        response.raise_for_status.side_effect = ConnectionError("down")
        response.status_code = "abc"
        with self.assertRaises(PipedriveError):
            raise_for_error(response)

    def test_raise_for_error_handles_non_json_error_body(self):
        response = MagicMock()
        response.status_code = 400
        response.raise_for_status.side_effect = requests.HTTPError("bad")
        response.json.side_effect = ValueError("not json")

        with self.assertRaises(Exception) as ctx:
            raise_for_error(response)

        self.assertIn("HTTP-error-code: 400", str(ctx.exception))

