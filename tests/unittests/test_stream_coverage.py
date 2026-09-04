import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import RequestException
from tap_pipedrive.exceptions import PipedriveForbiddenError

from tap_pipedrive.stream import (
    DynamicSchemaStream,
    PipedriveIncrementalStreamUsingSort,
    PipedriveIterStream,
    PipedriveStream,
)


class _BaseStream(PipedriveStream):
    schema = "deals"
    state_field = "update_time"
    key_properties = ["id"]


class _IterCoverageStream(PipedriveIterStream):
    schema = "dealflow"
    state_field = "log_time"
    base_endpoint = "deals"


class _SortCoverageStream(PipedriveIncrementalStreamUsingSort):
    schema = "notes"
    state_field = "update_time"


class _DynamicCoverageStream(DynamicSchemaStream):
    schema = "organizations"
    fields_endpoint = "organizationFields"


class _DummyTimer:
    def __init__(self):
        self.tags = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestStreamCoverageBranches(unittest.TestCase):
    def test_get_name_and_update_request_params_with_cursor_and_additional_fields(self):
        stream = _BaseStream()
        stream.endpoint = "deals"
        stream.initial_state = "2024-01-01T00:00:00Z"
        stream.cursor = "cursor-1"
        stream.additional_fields = ["a", "b"]

        self.assertEqual("deals", stream.get_name())
        params = stream.update_request_params({})
        self.assertEqual("cursor-1", params["cursor"])
        self.assertEqual("a,b", params["include_fields"])

    def test_check_access_returns_true_for_child_stream(self):
        stream = _BaseStream()
        stream.parent = "deals"
        self.assertTrue(stream.check_access())

    def test_check_access_returns_true_when_tap_missing(self):
        stream = _BaseStream()
        stream.parent = None
        stream.tap = None
        self.assertTrue(stream.check_access())

    def test_check_access_success_restores_initial_state(self):
        stream = _BaseStream()
        stream.endpoint = "deals"
        stream.initial_state = None
        stream.parent = None

        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.return_value = MagicMock()
        stream.tap = tap

        self.assertTrue(stream.check_access())
        self.assertIsNone(stream.initial_state)
        tap.execute_request.assert_called_once()

    def test_check_access_propagates_param_builder_error(self):
        stream = _BaseStream()
        stream.endpoint = "deals"
        stream.initial_state = "2024-05-01T00:00:00Z"
        stream.parent = None

        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        stream.tap = tap

        with patch.object(stream, "update_request_params", side_effect=RuntimeError("bad params")):
            with self.assertRaisesRegex(RuntimeError, "bad params"):
                stream.check_access()

        tap.execute_request.assert_not_called()
        self.assertEqual("2024-05-01T00:00:00Z", stream.initial_state)

    def test_check_access_returns_false_on_forbidden_and_restores_state(self):
        stream = _BaseStream()
        stream.endpoint = "deals"
        stream.initial_state = "2024-06-01T00:00:00Z"
        stream.parent = None

        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.side_effect = PipedriveForbiddenError("forbidden")
        stream.tap = tap

        self.assertFalse(stream.check_access())
        self.assertEqual("2024-06-01T00:00:00Z", stream.initial_state)

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    @patch("tap_pipedrive.stream.singer.write_bookmark", side_effect=lambda state, *args, **kwargs: state)
    def test_get_deal_ids_yields_ids_and_updates_bookmark(self, mock_write_bookmark, _timer):
        stream = _IterCoverageStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = True

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "data": [
                {
                    "id": 11,
                    "add_time": "2024-02-01T00:00:00Z",
                    "stage_change_time": None,
                    "update_time": "2024-02-01T00:00:00Z",
                }
            ],
            "additional_data": {},
        }

        tap = MagicMock()
        tap.state = {"bookmarks": {}}
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.return_value = response

        ids = list(stream.get_deal_ids(tap))

        self.assertEqual([11], ids)
        tap.validate_response.assert_called_once_with(response)
        tap.rate_throttling.assert_called_once_with(response)
        mock_write_bookmark.assert_called_once()

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_get_deal_ids_includes_cursor_param_when_present(self, _timer):
        stream = _IterCoverageStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = True
        stream.cursor = "next-cursor"

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": [], "additional_data": {}}

        tap = MagicMock()
        tap.state = {"bookmarks": {}}
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.return_value = response

        list(stream.get_deal_ids(tap))

        call_kwargs = tap.execute_request.call_args.kwargs
        self.assertEqual("next-cursor", call_kwargs["params"]["cursor"])

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_get_deal_ids_skips_empty_data(self, _timer):
        stream = _IterCoverageStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = True

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": [], "additional_data": {}}

        tap = MagicMock()
        tap.state = {"bookmarks": {}}
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.return_value = response

        self.assertEqual([], list(stream.get_deal_ids(tap)))

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_get_deal_ids_reraises_request_exception(self, _timer):
        stream = _IterCoverageStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = True

        tap = MagicMock()
        tap.state = {"bookmarks": {}}
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.side_effect = RequestException("network")

        with self.assertRaises(RequestException):
            list(stream.get_deal_ids(tap))

    def test_sort_stream_request_params_and_set_initial_state(self):
        stream = _SortCoverageStream()
        stream.cursor = "c-2"
        params = stream.update_request_params({})
        self.assertEqual("update_time", params["sort_by"])
        self.assertEqual("desc", params["sort_direction"])
        self.assertEqual("c-2", params["cursor"])

        stream._max_seen_bookmark = "2024-04-01T00:00:00Z"
        stream.set_initial_state({"bookmarks": {}}, "2024-01-01T00:00:00Z")
        self.assertIsNone(stream._max_seen_bookmark)

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_dynamic_schema_stream_populates_field_types_and_pagination(self, _timer):
        stream = _DynamicCoverageStream()
        stream.tap = MagicMock()

        page1 = MagicMock()
        page1.status_code = 200
        page1.json.return_value = {
            "data": [
                {"key": "int_field", "field_type": "int"},
                {"key": "double_field", "field_type": "double"},
                {"key": "set_field", "field_type": "set"},
                {"key": "monetary_field", "field_type": "monetary"},
                {"key": "date_field", "field_type": "date"},
                {"key": "json_field", "field_type": "text", "json_column_flag": True},
                {"key": "default_field", "field_type": "text"},
            ],
            "additional_data": {
                "pagination": {
                    "more_items_in_collection": True,
                    "next_start": 100,
                }
            },
        }
        page2 = MagicMock()
        page2.status_code = 200
        page2.json.return_value = {"data": [], "additional_data": {}}
        stream.tap.execute_request.side_effect = [page1, page2]

        schema = stream.get_schema()

        self.assertIn("int_field", schema["properties"])
        self.assertIn("integer", schema["properties"]["int_field"]["type"])
        self.assertIn("number", schema["properties"]["double_field"]["type"])
        self.assertIn("array", schema["properties"]["set_field"]["type"])
        self.assertIn("object", schema["properties"]["monetary_field"]["type"])
        self.assertEqual("date-time", schema["properties"]["date_field"]["format"])
        self.assertIn("object", schema["properties"]["json_field"]["type"])
        self.assertIn("string", schema["properties"]["default_field"]["type"])

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_dynamic_schema_stream_reraises_request_exception(self, _timer):
        stream = _DynamicCoverageStream()
        stream.tap = MagicMock()
        stream.tap.execute_request.side_effect = RequestException("network")

        with self.assertRaises(RequestException):
            stream.get_schema()

    @patch("tap_pipedrive.stream.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_dynamic_schema_stream_handles_json_parsing_exception_and_exits_loop(self, _timer):
        stream = _DynamicCoverageStream()
        stream.tap = MagicMock()

        response = MagicMock()
        response.status_code = 200

        def bad_json():
            stream.fields_more_items_in_collection = False
            raise ValueError("bad json")

        response.json.side_effect = bad_json
        stream.tap.execute_request.return_value = response

        schema = stream.get_schema()
        self.assertIn("properties", schema)

