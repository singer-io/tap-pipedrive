import unittest

from tap_pipedrive.stream import PipedriveStream
from tap_pipedrive.tap import PipedriveTap
from tap_pipedrive.exceptions import PipedriveForbiddenError


class FakeStream(PipedriveStream):
    endpoint = "fake"
    api_version = "v1"
    key_properties = ["id"]
    state_field = None
    replication_method = "FULL_TABLE"

    def __init__(self, schema_name, parent=None, accessible=True):
        self.schema = schema_name
        self.parent = parent
        self._accessible = accessible

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "id": {"type": ["integer", "null"]}
            }
        }

    def check_access(self):
        if self.parent:
            return True
        return self._accessible


class TestDiscoveryAccessChecks(unittest.TestCase):

    def setUp(self):
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "api_token": "abc",
        }
        self.tap = PipedriveTap(config, {})

    def test_discovery_keeps_all_accessible_streams(self):
        self.tap.streams = [
            FakeStream("deals", accessible=True),
            FakeStream("dealflow", parent="deals", accessible=True),
            FakeStream("users", accessible=True),
        ]

        catalog = self.tap.do_discover()
        discovered = {entry.tap_stream_id for entry in catalog.streams}

        self.assertSetEqual(discovered, {"deals", "dealflow", "users"})

    def test_discovery_excludes_inaccessible_parent_and_child(self):
        self.tap.streams = [
            FakeStream("deals", accessible=False),
            FakeStream("dealflow", parent="deals", accessible=True),
            FakeStream("users", accessible=True),
        ]

        catalog = self.tap.do_discover()
        discovered = {entry.tap_stream_id for entry in catalog.streams}

        self.assertSetEqual(discovered, {"users"})

    def test_discovery_raises_when_no_parent_stream_is_accessible(self):
        self.tap.streams = [
            FakeStream("deals", accessible=False),
            FakeStream("users", accessible=False),
            FakeStream("dealflow", parent="deals", accessible=True),
        ]

        with self.assertRaises(PipedriveForbiddenError) as error:
            self.tap.do_discover()

        self.assertEqual(
            str(error.exception),
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams.",
        )

    def test_child_stream_contains_parent_tap_stream_id_metadata(self):
        self.tap.streams = [
            FakeStream("deals", accessible=True),
            FakeStream("dealflow", parent="deals", accessible=True),
        ]

        catalog = self.tap.do_discover()
        dealflow_entry = next(s for s in catalog.streams if s.tap_stream_id == "dealflow")
        root_metadata = next(
            item["metadata"]
            for item in dealflow_entry.metadata
            if item["breadcrumb"] in ([], ())
        )

        self.assertEqual(root_metadata.get("parent-tap-stream-id"), "deals")
