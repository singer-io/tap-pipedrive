import unittest
from unittest.mock import patch

from tap_pipedrive.tap import PipedriveTap

from .base import FakeStream, MockResponse, PipedriveMockedBaseTest


class BookmarkMockedIntegrationTest(PipedriveMockedBaseTest, unittest.TestCase):
    @patch("tap_pipedrive.tap.singer.write_state")
    @patch("tap_pipedrive.tap.singer.write_record")
    @patch("tap_pipedrive.tap.PipedriveTap.validate_response")
    @patch("tap_pipedrive.tap.PipedriveTap.rate_throttling")
    def test_bookmark_advances_and_old_records_filtered(
        self,
        _mock_rate,
        _mock_validate,
        mock_write_record,
        _mock_write_state,
    ):
        stream = FakeStream(schema="deals", state_field="update_time")
        state = {"bookmarks": {"deals": {"update_time": "2024-01-02T00:00:00Z"}}}
        response = MockResponse(
            {
                "success": True,
                "data": [
                    {"id": 1, "name": "old", "update_time": "2024-01-01T00:00:00Z"},
                    {"id": 2, "name": "new", "update_time": "2024-01-03T00:00:00Z"},
                ],
                "additional_data": {"pagination": {"more_items_in_collection": False}},
            }
        )

        with patch.object(PipedriveTap, "streams", [stream]), \
             patch("tap_pipedrive.tap.PipedriveTap.execute_stream_request", return_value=response):
            tap = PipedriveTap(self.base_config(), state)
            catalog = self.select_all_streams(tap.do_discover())
            tap.do_sync(catalog)

        self.assertEqual(mock_write_record.call_count, 1)
        written_row = mock_write_record.call_args.args[1]
        self.assertEqual(written_row["id"], 2)
        self.assertTrue(tap.state["bookmarks"]["deals"]["update_time"].startswith("2024-01-03T00:00:00"))
