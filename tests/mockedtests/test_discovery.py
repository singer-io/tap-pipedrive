import unittest
from unittest.mock import patch

from singer import metadata

from tap_pipedrive.tap import PipedriveTap

from .base import FakeStream, PipedriveMockedBaseTest


class DiscoveryMockedIntegrationTest(PipedriveMockedBaseTest, unittest.TestCase):
    def test_discovery_stream_metadata(self):
        stream = FakeStream(schema="deals", state_field="update_time", replication_method="INCREMENTAL")

        with patch.object(PipedriveTap, "streams", [stream]):
            tap = PipedriveTap(self.base_config(), {})
            catalog = tap.do_discover()

        self.assertEqual(len(catalog.streams), 1)
        entry = catalog.streams[0]
        self.assertEqual(entry.tap_stream_id, "deals")

        mdata = metadata.to_map(entry.metadata)
        root = mdata.get((), {})
        self.assertEqual(root.get("forced-replication-method"), "INCREMENTAL")
        self.assertIn("id", root.get("table-key-properties", []))

        self.assertEqual(metadata.get(mdata, ("properties", "update_time"), "inclusion"), "automatic")
