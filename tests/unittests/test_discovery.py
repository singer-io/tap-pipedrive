import unittest
from unittest.mock import MagicMock, patch

from tap_pipedrive.stream import PipedriveStream
from tap_pipedrive.tap import PipedriveTap
from tap_pipedrive.exceptions import (
    PipedriveForbiddenError,
    PipedriveTooManyRequestsError,
    PipedriveUnauthorizedError,
)


class FakeStream(PipedriveStream):
    endpoint = "fake"
    api_version = "v1"
    key_properties = ["id"]
    state_field = None
    replication_method = "FULL_TABLE"

    def __init__(self, schema_name, parent=None):
        self.schema = schema_name
        self.parent = parent

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "id": {"type": ["integer", "null"]}
            }
        }

class TestDiscoveryAccessChecks(unittest.TestCase):

    def setUp(self):
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "api_token": "abc",
        }
        self.tap = PipedriveTap(config, {})
        self.tap.execute_request = MagicMock()

    def test_discovery_keeps_all_accessible_streams(self):
        self.tap.streams = [
            FakeStream("deals"),
            FakeStream("dealflow", parent="deals"),
            FakeStream("users"),
        ]

        catalog = self.tap.do_discover()
        discovered = {entry.tap_stream_id for entry in catalog.streams}

        self.assertSetEqual(discovered, {"deals", "dealflow", "users"})

    def test_discovery_excludes_forbidden_stream_and_keeps_authorized_streams(self):
        self.tap.streams = [
            FakeStream("deals"),
            FakeStream("users"),
        ]
        self.tap.execute_request.side_effect = [
            PipedriveForbiddenError("HTTP-error-code: 403, Error: insufficient permissions"),
            MagicMock(),
        ]

        with self.assertLogs("root", level="WARNING") as logs:
            catalog = self.tap.do_discover()
        discovered = {entry.tap_stream_id for entry in catalog.streams}

        self.assertSetEqual(discovered, {"users"})
        self.assertEqual(self.tap.execute_request.call_count, 2)
        self.assertIn("Unauthorized Stream: FakeStream", "\n".join(logs.output))
        self.assertIn("HTTP-error-code: 403", "\n".join(logs.output))

    def test_discovery_propagates_unauthorized_error_without_probing_more_streams(self):
        self.tap.streams = [FakeStream("deals"), FakeStream("users")]
        error = PipedriveUnauthorizedError("HTTP-error-code: 401, Error: invalid credentials")
        self.tap.execute_request.side_effect = error

        with patch("tap_pipedrive.stream.logger.warning") as warning:
            with self.assertRaisesRegex(PipedriveUnauthorizedError, "401"):
                self.tap.do_discover()

        self.tap.execute_request.assert_called_once()
        warning.assert_not_called()

    def test_discovery_propagates_non_authorization_error_without_excluding_stream(self):
        self.tap.streams = [FakeStream("deals"), FakeStream("users")]
        error = PipedriveTooManyRequestsError("HTTP-error-code: 429, Error: rate limited")
        self.tap.execute_request.side_effect = error

        with patch("tap_pipedrive.stream.logger.warning") as warning:
            with self.assertRaisesRegex(PipedriveTooManyRequestsError, "429"):
                self.tap.do_discover()

        self.tap.execute_request.assert_called_once()
        warning.assert_not_called()

    def test_discovery_raises_when_no_parent_stream_is_accessible(self):
        self.tap.streams = [
            FakeStream("deals"),
            FakeStream("users"),
            FakeStream("dealflow", parent="deals"),
        ]
        self.tap.execute_request.side_effect = PipedriveForbiddenError(
            "HTTP-error-code: 403, Error: insufficient permissions"
        )

        with self.assertLogs("root", level="WARNING") as logs:
            with self.assertRaisesRegex(PipedriveForbiddenError, "No accessible streams remain"):
                self.tap.do_discover()

        self.assertEqual(self.tap.execute_request.call_count, 2)
        output = "\n".join(logs.output)
        self.assertIn("Stream 'dealflow' excluded", output)
        self.assertIn("No 'read' access to stream(s): deals, users", output)

    def test_child_stream_contains_parent_tap_stream_id_metadata(self):
        self.tap.streams = [
            FakeStream("deals"),
            FakeStream("dealflow", parent="deals"),
        ]

        catalog = self.tap.do_discover()
        dealflow_entry = next(s for s in catalog.streams if s.tap_stream_id == "dealflow")
        root_metadata = next(
            item["metadata"]
            for item in dealflow_entry.metadata
            if item["breadcrumb"] in ([], ())
        )

        self.assertEqual(root_metadata.get("parent-tap-stream-id"), "deals")
