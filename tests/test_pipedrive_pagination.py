from tap_tester import runner, menagerie, connections

from base import PipeDriveBaseTest

class PaginationTest(PipeDriveBaseTest):

    @staticmethod
    def name():
        return "tap_tester_pipedrive_pagination_test"

    def test_run(self):
        page_size = 100
        conn_id = connections.ensure_connection(self)

        expected_streams = ["deals"]
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        test_catalogs = [catalog for catalog in found_catalogs
                                      if catalog.get('stream') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                expected_primary_keys = self.expected_pks()

                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']

                self.assertGreater(record_count_sync, page_size)

                if record_count_sync > page_size:
                    primary_keys_list_1 = primary_keys_list[:page_size]
                    primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                    primary_keys_page_1 = set(primary_keys_list_1)
                    primary_keys_page_2 = set(primary_keys_list_2)

                    self.assertTrue(primary_keys_page_1.isdisjoint(primary_keys_page_2))