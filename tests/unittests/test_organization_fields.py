import unittest
from unittest.mock import MagicMock, patch

from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_pipedrive.streams.organization_fields import OrganizationFieldsStream
from tap_pipedrive.tap import PipedriveTap


def _make_response(payload):
    response = MagicMock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = payload
    return response


class TestOrganizationFieldsAttributes(unittest.TestCase):
    """
    The Pipedrive API v2 organizationFields response does not include an
    `id`. `field_code` is the only identifier present on every record
    (standard and custom fields alike), so it must be the primary key.
    """

    def test_get_name_returns_schema(self):
        stream = OrganizationFieldsStream()
        self.assertEqual("organization_fields", stream.get_name())

    def test_field_code_is_the_primary_key(self):
        stream = OrganizationFieldsStream()
        self.assertEqual(["field_code"], stream.key_properties)

    def test_replication_method_is_full_table_with_no_replication_key(self):
        stream = OrganizationFieldsStream()
        self.assertEqual("FULL_TABLE", stream.replication_method)
        self.assertIsNone(stream.state_field)


class TestOrganizationFieldsUpdateRequestParams(unittest.TestCase):
    def test_update_request_params_without_cursor(self):
        stream = OrganizationFieldsStream()
        stream.cursor = None
        params = stream.update_request_params({})
        self.assertEqual({"limit": stream.limit}, params)

    def test_update_request_params_with_cursor(self):
        stream = OrganizationFieldsStream()
        stream.cursor = "org-fields-cursor"
        params = stream.update_request_params({})
        self.assertEqual({"limit": stream.limit, "cursor": "org-fields-cursor"}, params)


class TestOrganizationFieldsProcessRow(unittest.TestCase):
    """
    Unlike the v1-style dealFields/organizationFields shape, the v2
    organizationFields response has no separate "child" rows without a
    primary key - subfields are nested inside `subfields` on the parent
    record itself. So process_row is just the inherited passthrough.
    """

    def test_standard_field_is_passed_through_unchanged(self):
        stream = OrganizationFieldsStream()
        row = {
            "field_name": "Name",
            "field_code": "name",
            "field_type": "varchar",
            "options": None,
            "subfields": None,
            "is_custom_field": False,
            "is_optional_response_field": False,
            "is_writable": True,
        }
        self.assertEqual(row, stream.process_row(row))

    def test_field_with_options_is_passed_through_unchanged(self):
        stream = OrganizationFieldsStream()
        row = {
            "field_name": "Industry",
            "field_code": "industry",
            "field_type": "enum",
            "options": [
                {"id": 1, "label": "Accommodation services"},
                {"id": 2, "label": "Administrative and support services"},
            ],
            "subfields": None,
            "is_custom_field": False,
            "is_optional_response_field": False,
            "is_writable": True,
        }
        self.assertEqual(row, stream.process_row(row))

    def test_field_with_subfields_is_passed_through_unchanged(self):
        stream = OrganizationFieldsStream()
        row = {
            "field_name": "Address",
            "field_code": "address",
            "field_type": "address",
            "options": None,
            "subfields": [
                {"field_code": "value", "field_name": "Address value of Address", "field_type": "varchar"},
                {"field_code": "street_number", "field_name": "House number of Address", "field_type": "varchar"},
            ],
            "is_custom_field": False,
            "is_optional_response_field": False,
            "is_writable": True,
        }
        self.assertEqual(row, stream.process_row(row))

    def test_custom_field_is_passed_through_unchanged(self):
        stream = OrganizationFieldsStream()
        row = {
            "field_name": "Industry",
            "field_code": "40characterhashforcustomfieldidentifier",
            "field_type": "enum",
            "options": [{"id": 1, "label": "Technology"}],
            "subfields": None,
            "is_custom_field": True,
            "is_optional_response_field": False,
            "is_writable": True,
        }
        self.assertEqual(row, stream.process_row(row))

    def test_nullable_options_and_subfields_are_preserved_as_none(self):
        stream = OrganizationFieldsStream()
        row = {
            "field_name": "Owner",
            "field_code": "owner_id",
            "field_type": "user",
            "options": None,
            "subfields": None,
            "is_custom_field": False,
            "is_optional_response_field": False,
            "is_writable": True,
        }
        result = stream.process_row(row)
        self.assertIsNone(result["options"])
        self.assertIsNone(result["subfields"])


class TestOrganizationFieldsFullSync(unittest.TestCase):
    """
    Integration-style test driving tap.do_sync end to end, verifying
    cursor pagination via additional_data.next_cursor and that field_code
    (not id) is used to identify records written to the stream.
    """

    def _execute_request_side_effect(self, endpoint, api_version, params=None):
        params = params or {}
        cursor = params.get("cursor")

        if endpoint == "organizationFields":
            if cursor is None:
                return _make_response({
                    "success": True,
                    "data": [
                        {
                            "field_name": "Name",
                            "field_code": "name",
                            "field_type": "varchar",
                            "options": None,
                            "subfields": None,
                            "is_custom_field": False,
                            "is_optional_response_field": False,
                            "is_writable": True,
                        },
                        {
                            "field_name": "Address",
                            "field_code": "address",
                            "field_type": "address",
                            "options": None,
                            "subfields": [
                                {"field_code": "value", "field_name": "Address value of Address", "field_type": "varchar"},
                            ],
                            "is_custom_field": False,
                            "is_optional_response_field": False,
                            "is_writable": True,
                        },
                    ],
                    "additional_data": {"next_cursor": "of-cursor-1"},
                })
            if cursor == "of-cursor-1":
                return _make_response({
                    "success": True,
                    "data": [
                        {
                            "field_name": "Industry",
                            "field_code": "40characterhashforcustomfieldidentifier",
                            "field_type": "enum",
                            "options": [{"id": 1, "label": "Technology"}],
                            "subfields": None,
                            "is_custom_field": True,
                            "is_optional_response_field": False,
                            "is_writable": True,
                        },
                    ],
                    "additional_data": {"next_cursor": None},
                })

        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    def _build_catalog(self, stream):
        schema_dict = stream.get_schema()
        schema = Schema.from_dict(schema_dict)
        meta = metadata.get_standard_metadata(
            schema=schema_dict,
            key_properties=stream.key_properties,
            replication_method=stream.replication_method,
        )

        return Catalog([CatalogEntry(
            stream=stream.schema,
            tap_stream_id=stream.schema,
            key_properties=stream.key_properties,
            schema=schema,
            metadata=meta,
        )])

    def test_do_sync_paginates_and_writes_records_keyed_by_field_code(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {"bookmarks": {}})
        stream = OrganizationFieldsStream()
        tap.streams = [stream]

        catalog = self._build_catalog(stream)

        with patch.object(tap, "execute_request", side_effect=self._execute_request_side_effect), \
             patch("tap_pipedrive.tap.PipedriveTap.get_selected_streams", return_value=["organization_fields"]), \
             patch("tap_pipedrive.tap.singer.write_record") as mock_write_record, \
             patch("tap_pipedrive.tap.singer.write_state"):
            tap.do_sync(catalog)

        written_records = [call.args[1] for call in mock_write_record.call_args_list]
        field_codes = sorted(row["field_code"] for row in written_records)

        self.assertEqual(
            ["40characterhashforcustomfieldidentifier", "address", "name"],
            field_codes,
        )

        address_row = next(row for row in written_records if row["field_code"] == "address")
        self.assertEqual("value", address_row["subfields"][0]["field_code"])

        custom_row = next(
            row for row in written_records if row["field_code"] == "40characterhashforcustomfieldidentifier"
        )
        self.assertTrue(custom_row["is_custom_field"])
        self.assertEqual(1, custom_row["options"][0]["id"])
