import unittest
from unittest.mock import patch

from tap_pipedrive.tap import PipedriveTap

from .base import FakeStream, MockResponse, PipedriveMockedBaseTest


class AllFieldsMockedIntegrationTest(PipedriveMockedBaseTest, unittest.TestCase):
    @patch("tap_pipedrive.tap.singer.write_state")
    @patch("tap_pipedrive.tap.singer.write_record")
    @patch("tap_pipedrive.tap.PipedriveTap.validate_response")
    @patch("tap_pipedrive.tap.PipedriveTap.rate_throttling")
    def test_all_fields_and_custom_fields_are_written(
        self,
        _mock_rate,
        _mock_validate,
        mock_write_record,
        _mock_write_state,
    ):
        stream = FakeStream(schema="deals", state_field="update_time")
        stream.get_schema = lambda: {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "integer"]},
                "name": {"type": ["null", "string"]},
                "update_time": {"type": ["null", "string"], "format": "date-time"},
                "cf_text": {"type": ["null", "string"]},
                "cf_num": {"type": ["null", "integer"]},
            },
        }
        response = MockResponse(
            {
                "success": True,
                "data": [
                    {
                        "id": 42,
                        "name": "Deal 42",
                        "update_time": "2024-01-12T00:00:00Z",
                        "custom_fields": {"cf_text": "hello", "cf_num": 10},
                    }
                ],
                "additional_data": {"pagination": {"more_items_in_collection": False}},
            }
        )

        with patch.object(PipedriveTap, "streams", [stream]), \
             patch("tap_pipedrive.tap.PipedriveTap.execute_stream_request", return_value=response):
            tap = PipedriveTap(self.base_config(start_date="2024-01-01T00:00:00Z"), {})
            catalog = self.select_all_streams(tap.do_discover())
            tap.do_sync(catalog)

        self.assertEqual(mock_write_record.call_count, 1)
        written = mock_write_record.call_args.args[1]
        self.assertEqual(written["id"], 42)
        self.assertEqual(written["name"], "Deal 42")
        self.assertTrue(written["update_time"].startswith("2024-01-12T00:00:00"))
        self.assertEqual(written["cf_text"], "hello")
        self.assertEqual(written["cf_num"], 10)
