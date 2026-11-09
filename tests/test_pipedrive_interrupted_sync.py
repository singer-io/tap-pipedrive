from base import PipedriveBaseTest
from tap_tester import runner, connections, menagerie


class PipedriveInterruptedSyncTest(PipedriveBaseTest):
    """
    Test the tap's ability to recover from an interrupted sync.

    The tap honours `currently_syncing` in the state by skipping every selected
    stream that precedes it in the tap's stream order, resuming the interrupted
    stream from its bookmark, and syncing the remaining streams normally.
    """

    # Order in which PipedriveTap.streams syncs streams (see tap_pipedrive/tap.py)
    TAP_STREAM_ORDER = [
        'currency', 'activity_types', 'stages', 'filters', 'pipelines', 'notes',
        'users', 'activities', 'deals', 'files', 'organizations', 'organization_fields',
        'persons', 'products', 'product_variations', 'dealflow', 'deal_products',
        'deal_fields', 'deal_installments'
    ]

    def name(self):
        return "tap_tester_pipedrive_interrupted_sync_test"

    def test_run(self):
        """
        Scenario: First sync is interrupted part way through an incremental stream.
        - Verify an interrupted sync can resume using `currently_syncing` in the state
        - Verify `currently_syncing` is cleared from the state once the resumed sync completes
        - Verify streams completed before the interruption are skipped by the resumed sync
          and their bookmarks are preserved
        - Verify the interrupted stream replicates exactly the records at or after its saved bookmark
        - Verify streams yet to be synced at the time of interruption replicate the same
          records as a full sync and record their bookmark

        Bookmark semantics shared with a normal sync (bookmark is the max replication key,
        full table streams record no bookmark, records respect the bookmark) are covered by
        test_pipedrive_bookmarks.py and intentionally not repeated here.
        """

        # BUG TDL-25987: We observed few records with null replication key values for below streams
        # Skipping this stream until we investigate and fix the issue
        expected_streams = self.expected_streams() - {"deal_fields", "stages", "pipelines", "filters", "activity_types"}

        # Skipping below streams it make lot of API calls and we have daily quota limit for trail account
        # Skipping deal_installments as it also requires a Pipedrive Growth plan or above (403 otherwise)
        expected_streams = expected_streams - {"dealflow", "deal_products", "product_variations", "deal_installments"}

        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        expected_primary_keys = self.expected_primary_keys()

        ordered_streams = [stream for stream in self.TAP_STREAM_ORDER if stream in expected_streams]

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        ##########################################################################
        ### Full Sync
        ##########################################################################

        self.run_and_verify_sync(conn_id)
        full_sync_records = runner.get_records_from_target_output()
        full_sync_bookmarks = menagerie.get_state(conn_id).get('bookmarks', {})

        ##########################################################################
        ### Pick Interrupted Stream
        ##########################################################################

        # The interrupted stream must be incremental (the tap only sets `currently_syncing` for
        # streams with a replication key), must have streams before and after it so every branch
        # of the resume logic is exercised, and needs at least 2 distinct replication key values
        # (at bookmark precision) so a mid-point bookmark yields a strict, non-empty subset.
        # Seeded test data is often created in bulk with identical timestamps, so pick dynamically.
        interrupted_stream = None
        for stream in ordered_streams[1:-1]:
            if expected_replication_methods[stream] != self.INCREMENTAL:
                continue
            replication_key = next(iter(expected_replication_keys[stream]))
            replication_key_values = sorted({
                self.parse_date(message['data'][replication_key]).strftime(self.BOOKMARK_FORMAT)
                for message in full_sync_records.get(stream, {}).get('messages', [])
                if message.get('action') == 'upsert' and message['data'].get(replication_key)
            })
            if len(replication_key_values) > 1:
                interrupted_stream = stream
                break

        self.assertIsNotNone(interrupted_stream,
                             msg="No incremental stream with at least 2 distinct replication key values to interrupt on")
        self.LOGGER.info("Simulating interruption on stream: %s", interrupted_stream)

        interrupted_index = ordered_streams.index(interrupted_stream)
        completed_streams = set(ordered_streams[:interrupted_index])

        ##########################################################################
        ### Build Interrupted State
        ##########################################################################

        interrupted_state = {
            'currently_syncing': interrupted_stream,
            'bookmarks': {}
        }

        # Completed streams keep the bookmark they wrote in the full sync
        for stream in completed_streams:
            if expected_replication_methods[stream] == self.INCREMENTAL:
                interrupted_state['bookmarks'][stream] = full_sync_bookmarks[stream]

        # The interrupted stream gets a bookmark in the middle of the records from the
        # full sync, so the resumed sync should replicate a strict, non-empty subset of them.
        simulated_bookmark = replication_key_values[len(replication_key_values) // 2]
        interrupted_state['bookmarks'][interrupted_stream] = {replication_key: simulated_bookmark}

        # Pending streams have no bookmark yet, they were never reached by the interrupted sync

        menagerie.set_state(conn_id, interrupted_state)

        ##########################################################################
        ### Resumed Sync
        ##########################################################################

        resumed_sync_record_count = self.run_and_verify_sync(conn_id)
        resumed_sync_records = runner.get_records_from_target_output()
        resumed_sync_state = menagerie.get_state(conn_id)
        resumed_sync_bookmarks = resumed_sync_state.get('bookmarks', {})

        # Verify `currently_syncing` is cleared once the resumed sync completes
        self.assertNotIn('currently_syncing', resumed_sync_state,
                         msg="`currently_syncing` should be cleared after a successful resumed sync")

        for stream in expected_streams:
            with self.subTest(stream=stream):

                primary_keys = expected_primary_keys[stream]

                full_sync_messages = [message['data'] for message in
                                      full_sync_records.get(stream, {}).get('messages', [])
                                      if message.get('action') == 'upsert']
                resumed_sync_messages = [message['data'] for message in
                                         resumed_sync_records.get(stream, {}).get('messages', [])
                                         if message.get('action') == 'upsert']

                full_sync_pks = {tuple(record.get(pk) for pk in primary_keys) for record in full_sync_messages}
                resumed_sync_pks = {tuple(record.get(pk) for pk in primary_keys) for record in resumed_sync_messages}

                full_sync_bookmark = full_sync_bookmarks.get(stream)
                resumed_sync_bookmark = resumed_sync_bookmarks.get(stream)

                if stream in completed_streams:
                    # Verify streams that completed before the interruption are skipped
                    self.assertEqual(resumed_sync_record_count.get(stream, 0), 0,
                                     msg="Stream {} completed before the interruption but was re-synced".format(stream))

                    # Verify the bookmark for skipped streams is carried over untouched
                    self.assertEqual(resumed_sync_bookmark, full_sync_bookmark)

                elif stream == interrupted_stream:
                    # Verify the resumed sync replicates exactly the full-sync records at or after the saved bookmark
                    simulated_bookmark_dt = self.parse_date(simulated_bookmark)
                    expected_resumed_pks = {
                        tuple(record.get(pk) for pk in primary_keys) for record in full_sync_messages
                        if record.get(replication_key)
                        and self.parse_date(record[replication_key]) >= simulated_bookmark_dt
                    }
                    self.assertSetEqual(resumed_sync_pks, expected_resumed_pks)

                else:
                    # Verify streams not yet reached by the interrupted sync replicate everything
                    self.assertSetEqual(resumed_sync_pks, full_sync_pks,
                                        msg="Pending stream {} should replicate the same records as a full sync".format(stream))

                    # Verify pending streams record their bookmark
                    self.assertEqual(resumed_sync_bookmark, full_sync_bookmark)  # assumes no changes to data during test
