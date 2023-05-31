from tap_tester import connections, menagerie, runner
from base import PipedriveBaseTest


class PipedriveAllFields(PipedriveBaseTest):

    """Ensure running the tap with all streams and fields selected results in the replication of all fields."""

    # Custom fields, not able to generate data for these fields.
    fields_to_remove = {
        "organizations": {
            "timeline_last_activity_time",
            "timeline_last_activity_time_by_owner",
            "reference_activities_count",
        },
        "persons": {
            "timeline_last_activity_time",
            "timeline_last_activity_time_by_owner",
            "reference_activities_count",
        },
        "files": {"email_message_id", "note_id"},
        "users": {"activated"},
        "deals": {
            "product_amount",
            "pipeline",
            "reference_activities_count",
            "product_quantity",
        },
        "products": {"unit_prices", "price"},
    }

    def name(self):
        return "tap_tester_pipedrive_all_fields"

    def test_all_fields_run(self):

        """
        Assert given conditions:
        • verify no unexpected streams are replicated
        • Verify that more than just the automatic fields are replicated for each stream.
        • Verify all fields for each streams are replicated
        """

        # Streams to verify all fields tests
        expected_streams = self.expected_streams()

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get("tap_stream_id") in expected_streams]

        # perform table and field selections
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table and field selection to set expections
        # used for asserting all fields are replicated
        stream_to_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [metadata["breadcrumb"][1] for metadata in catalog_entry["metadata"]
                                          if metadata["breadcrumb"] != []]
            stream_to_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run sync mode
        stream_to_record_count = self.run_and_verify_sync(conn_id)

        # get records from target output
        synced_recs = runner.get_records_from_target_output()

        # verify no unexpected streams are replicated
        synced_streams_names = set(stream_to_record_count.keys())
        self.assertSetEqual(expected_streams, synced_streams_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values for stream
                expected_all_keys = stream_to_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields[stream]

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                # actual values
                actual_all_keys = set()
                for message in synced_recs.get(stream).get("messages"):
                    if message["action"] == "upsert":
                        actual_all_keys = actual_all_keys.union(
                            set(message["data"].keys())
                        )

                # verify all fields for each stream are replicated
                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove.get(stream, [])
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertGreaterEqual(
                    expected_all_keys,
                    actual_all_keys,
                    msg=f"Some expected keys are not there in the set of actual keys for the {stream} stream",
                )
