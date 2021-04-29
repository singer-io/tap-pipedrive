from tap_tester import connections, menagerie, runner
import unittest
import os

class PipedriveDiscovery(unittest.TestCase):
    def environment_variables(self):
        return {"TAP_PIPEDRIVE_API_TOKEN"}

    def setUp(self):
        missing_envs = [x for x in self.environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing test-required environment variables: {}".format(missing_envs))

    def expected_check_streams(self):
        return {
            'files',
            'activities',
            'dealflow',
            'deal_products',
            'activity_types',
            'persons',
            'currency',
            'pipelines',
            'notes',
            'stages',
            'products',
            'organizations',
            'users',
            'delete_log',
            'filters',
            'deals'
        }

    def expected_pks(self):
        return {'activities': {'id'},
                'activity_types': {'id'},
                'currency': {'id'},
                'deal_products': {'id'},
                'dealflow': {'id'},
                'deals': {'id'},
                'delete_log': {'id'},
                'files': {'id'},
                'filters': {'id'},
                'notes': {'id'},
                'organizations': {'id'},
                'persons': {'id'},
                'pipelines': {'id'},
                'products': {'id'},
                'stages': {'id'},
                'users': {'id'}}

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-pipedrive"

    @staticmethod
    def get_type():
        return "platform.pipedrive"

    def name(self):
        return "tap_tester_pipedrive_discovery"

    def get_credentials(self):
        return {'api_token': os.getenv('TAP_PIPEDRIVE_API_TOKEN')}

    def get_properties(self):
        return {'start_date' : "2019-09-21T00:00:00Z"}

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams

        # To run in local ( latest tap-tester )
        # catalog = menagerie.get_catalogs(conn_id)

        # To run in CCI
        catalog = menagerie.get_catalog(conn_id)
        catalog = catalog['stream']

        print(catalog)

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in catalog})

        known_replication_keys = {
            'persons': ['update_time'],
            'stages': ['update_time'],
            'files': ['update_time'],
            'activity_types': ['update_time'],
            'deal_products': [],
            'pipelines': ['update_time'],
            'dealflow': ['log_time'],
            'users': [],
            'activities': ['update_time'],
            'delete_log': [],
            'currency': [],
            'products': ['update_time'],
            'filters': ['update_time'],
            'notes': ['update_time'],
            'organizations': ['update_time'],
            'deals': ['update_time']}

        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in catalog if c['tap_stream_id'] == tap_stream_id][0]
            print(found_stream)
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
            non_key_property_metadata = [m for m in stream_metadata if m['breadcrumb'][1] not in (self.expected_pks()[tap_stream_id].union(known_replication_keys[tap_stream_id]))]
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