from base import PipedriveBaseTest
from tap_tester import runner, connections, menagerie

class PipedriveBookmarksTest(PipedriveBaseTest):

    def name(self):
        return "tap_tester_pipedrive_bookmarks_test"

    def test_run(self):
        """
        Testing that the bookmarking for the tap works as expected
        - Verify for each incremental stream you can do a sync which records bookmarks
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark
        - All data of the 2nd sync is >= the bookmark from the first sync
        - The number of records in the 2nd sync is less then the first
        """

        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        # BUG TDL-25987: We observed few records with null replication key values for deal_fields stream
        # Skipping this stream until we investigate and fix the issue
        expected_streams = self.expected_streams() - {"deal_fields"}
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                            if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State
        ##########################################################################
        new_state = {'bookmarks': dict()}
        simulated_states = {
            "notes": {"update_time": "2025-01-21T00:00:00+00:00"},
            "activities": {"update_time": "2025-01-21T00:00:00+00:00"},
            "deals": {"update_time": "2025-01-21T00:00:00+00:00"},
            "files": {"update_time": "2025-01-21T00:00:00+00:00"},
            "organizations": {"update_time": "2025-01-21T00:00:00+00:00"},
            "persons": {"update_time": "2025-01-21T00:00:00+00:00"},
            "products": {"update_time": "2025-01-21T00:00:00+00:00"},
            "dealflow": {"log_time": "2025-01-21T00:00:00+00:00"},

            # BUG TDL-25987: We observed few records with null replication key values for deal_fields stream
            # "deal_fields": {"update_time": "2023-04-15T17:25:16+00:00"}
        }
        # setting 'second_start_date' as bookmark for running 2nd sync
        for stream, updated_state in simulated_states.items():
            new_state['bookmarks'][stream] = updated_state

        # Set state for next sync
        menagerie.set_state(conn_id, new_state)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:
                    # dealflow stores bookmark in the format of "2022-05-27T04:16:22.410711+00:00"
                    # as, we subtract 3 hours from sync start date in current implementation
                    bookmark_format = "%Y-%m-%dT%H:%M:%S.%f+00:00" if stream == "dealflow" else self.BOOKMARK_FORMAT

                    # collect information specific to incremental streams from syncs 1 & 2
                    # Key in which state has been saved in state file
                    replication_key = list(expected_replication_keys[stream])[0]

                    first_bookmark_value = first_bookmark_key_value.get(replication_key) if first_bookmark_key_value else None
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)

                    first_bookmark_value_ts = self.dt_to_ts(first_bookmark_value, self.BOOKMARK_FORMAT)

                    second_bookmark_value_ts = self.dt_to_ts(second_bookmark_value, self.BOOKMARK_FORMAT)

                    simulated_bookmark_value = self.dt_to_ts(new_state['bookmarks'][stream][replication_key], self.BOOKMARK_FORMAT)

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_value)

                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_ts = self.dt_to_ts(replication_key_value, self.REPLICATION_KEY_FORMAT)
                        self.assertLessEqual(
                            replication_key_value_ts, first_bookmark_value_ts,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = record.get(replication_key)
                        replication_key_value_ts = self.dt_to_ts(replication_key_value, self.REPLICATION_KEY_FORMAT)
                        self.assertGreaterEqual(replication_key_value_ts, simulated_bookmark_value,
                                                msg="Second sync records do not respect the previous bookmark.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value_ts, second_bookmark_value_ts,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    # verify that you get less data the 2nd time around
                    self.assertLess(
                        second_sync_count,
                        first_sync_count,
                        msg="second sync didn't have less records, bookmark usage not verified")

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method)
                    )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
