from tap_tester import connections, menagerie, runner
import unittest
import os
from base import PipeDriveBaseTest

class PipedriveDiscovery(PipeDriveBaseTest):

    @staticmethod
    def name():
        return "tap_tester_pipedrive_discovery"

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run and verify the tap in discovermode. 
        catalog = self.run_and_verify_check_mode(conn_id)

        for tap_stream_id in self.expected_streams():
            found_stream = [c for c in catalog if c['tap_stream_id'] == tap_stream_id][0]
            # print(found_stream)
            found_key_properties = set(found_stream['key_properties'])
            stream_metadata = found_stream['metadata']

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[tap_stream_id],
                             found_key_properties)

            # Check that key properties have inclusion automatic
            for prop in found_key_properties:
                metadata_entries = [m['metadata'] for m in stream_metadata if m['breadcrumb'] == ['properties', prop]]
                self.assertEqual(len(metadata_entries), 1,
                                 msg="Found more or less than one metadata entry for key property {} in stream {}".format(
                                     prop, tap_stream_id))
                self.assertEqual(metadata_entries[0]['inclusion'], 'automatic',
                                 msg="Inclusion for key property {} in stream {} is not automatic".format(
                                     prop, tap_stream_id))

            # Check that all else have inclusion available
            non_key_property_metadata = [m for m in stream_metadata if m['breadcrumb'][1] not in (self.expected_pks()[tap_stream_id].union(self.known_replication_keys()[tap_stream_id]))]
            for metadata_entry in non_key_property_metadata:
                inclusion = metadata_entry['metadata']['inclusion']
                self.assertEqual(inclusion, 'available',
                                 msg="Inclusion for property {} in stream {} is not available, was: {}".format(
                                     metadata_entry['breadcrumb'][1],
                                     tap_stream_id,
                                     inclusion))

            # Verify after discovery that selected is None for all fields
            for metadata_entry in stream_metadata:
                self.assertNotIn('selected', metadata_entry['metadata'].keys())