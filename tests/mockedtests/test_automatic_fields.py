import unittest
from unittest.mock import patch

from singer import metadata

from tap_pipedrive.tap import PipedriveTap

from .base import FakeStream, PipedriveMockedBaseTest


class AutomaticFieldsMockedIntegrationTest(PipedriveMockedBaseTest, unittest.TestCase):
    def test_primary_and_replication_keys_are_automatic(self):
        stream = FakeStream(schema="deals", state_field="update_time", replication_method="INCREMENTAL")

        with patch.object(PipedriveTap, "streams", [stream]):
            tap = PipedriveTap(self.base_config(), {})
            catalog = tap.do_discover()

        entry = catalog.streams[0]
        mdata = metadata.to_map(entry.metadata)

        self.assertEqual(metadata.get(mdata, ("properties", "id"), "inclusion"), "automatic")
        self.assertEqual(metadata.get(mdata, ("properties", "update_time"), "inclusion"), "automatic")
        self.assertEqual(metadata.get(mdata, ("properties", "name"), "inclusion"), "available")
