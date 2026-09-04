import unittest

from tap_pipedrive.stream import PipedriveStream


class BookmarkTestStream(PipedriveStream):
    schema = "deals"
    state_field = "update_time"


class TestBookmarks(unittest.TestCase):

    def setUp(self):
        self.stream = BookmarkTestStream()

    def test_set_initial_state_reads_existing_bookmark(self):
        state = {
            "bookmarks": {
                "deals": {
                    "update_time": "2024-01-02T00:00:00Z"
                }
            }
        }

        self.stream.set_initial_state(state, "2024-01-01T00:00:00Z")

        self.assertEqual(self.stream.initial_state, "2024-01-02T00:00:00Z")
        self.assertEqual(self.stream.earliest_state, "2024-01-02T00:00:00Z")

    def test_set_initial_state_falls_back_to_start_date(self):
        self.stream.set_initial_state({}, "2024-01-01T00:00:00Z")

        self.assertEqual(self.stream.initial_state, "2024-01-01T00:00:00Z")
        self.assertEqual(self.stream.earliest_state, "2024-01-01T00:00:00Z")

    def test_update_state_advances_bookmark(self):
        self.stream.earliest_state = "2024-01-01T00:00:00Z"
        row = {"update_time": "2024-01-03T00:00:00.000000Z"}

        self.stream.update_state(row)

        self.assertEqual(self.stream.earliest_state, "2024-01-03T00:00:00Z")

    def test_update_state_does_not_regress_bookmark(self):
        self.stream.earliest_state = "2024-01-03T00:00:00Z"
        row = {"update_time": "2024-01-02T00:00:00.000000Z"}

        self.stream.update_state(row)

        self.assertEqual(self.stream.earliest_state, "2024-01-03T00:00:00Z")

    def test_write_record_filters_records_older_than_bookmark(self):
        self.stream.initial_state = "2024-01-03T00:00:00Z"

        self.assertFalse(self.stream.write_record({"update_time": "2024-01-02T00:00:00Z"}))
        self.assertTrue(self.stream.write_record({"update_time": "2024-01-03T00:00:00Z"}))
        self.assertTrue(self.stream.write_record({"update_time": "2024-01-04T00:00:00Z"}))
