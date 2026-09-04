import unittest
from unittest import mock

from tap_pipedrive.stream import PipedriveStream
from tap_pipedrive.tap import PipedriveTap


class SyncTestStream(PipedriveStream):
    schema = "users"
    endpoint = "users"
    state_field = "update_time"
    replication_method = "INCREMENTAL"
    key_properties = ["id"]

    def has_data(self):
        return False

    def write_schema(self):
        return None


class StreamCatalogEntry:
    schema = {"users": "users"}
    metadata = []


class FakeCatalog:
    streams = []

    def get_stream(self, stream_name):
        return StreamCatalogEntry()


class TestSync(unittest.TestCase):

    def setUp(self):
        self.config = {
            "api_token": "abc",
            "start_date": "2024-01-01T00:00:00Z",
        }
        self.tap = PipedriveTap(self.config, {})
        self.tap.streams = [SyncTestStream()]

    @mock.patch("singer.write_state")
    @mock.patch("tap_pipedrive.tap.PipedriveTap.get_selected_streams", return_value=["users"])
    def test_sync_sets_and_clears_currently_syncing(self, mocked_selected_streams, mocked_write_state):
        self.tap.do_sync(FakeCatalog())

        self.assertNotIn("currently_syncing", self.tap.state)
        self.assertEqual(
            self.tap.state.get("bookmarks", {}).get("users", {}).get("update_time"),
            "2024-01-01T00:00:00Z",
        )
