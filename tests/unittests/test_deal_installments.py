import unittest
from unittest.mock import MagicMock, patch

from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_pipedrive.exceptions import PipedriveBadRequestError, PipedriveForbiddenError
from tap_pipedrive.streams import CurrenciesStream
from tap_pipedrive.streams.deal_installments import DealInstallmentsStream
from tap_pipedrive.tap import PipedriveTap


class _DummyTimer:
    def __init__(self):
        self.tags = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _make_response(payload):
    response = MagicMock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = payload
    return response


class TestDealInstallmentsGetChildIds(unittest.TestCase):
    """Unit tests for DealInstallmentsStream.get_child_ids (deal id batching)."""

    @patch("tap_pipedrive.streams.deal_installments.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_yields_one_batch_per_page_of_deal_ids(self, _timer):
        stream = DealInstallmentsStream()
        tap = MagicMock()
        tap.execute_request.return_value = _make_response({
            "success": True,
            "data": [{"id": 1}, {"id": 2}, {"id": 3}],
            "additional_data": {"next_cursor": None},
        })

        batches = list(stream.get_child_ids(tap))

        self.assertEqual([[1, 2, 3]], batches)
        self.assertEqual([[1, 2, 3]], stream.child_ids)
        self.assertFalse(stream.more_ids_to_get)
        # deal-listing (parent) pagination must not touch the installments (child) cursor.
        self.assertIsNone(stream.cursor)

        call_kwargs = tap.execute_request.call_args.kwargs
        self.assertEqual("deals", tap.execute_request.call_args.args[0])
        self.assertEqual("api/v2", call_kwargs["api_version"])
        self.assertNotIn("cursor", call_kwargs["params"])

    @patch("tap_pipedrive.streams.deal_installments.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_paginates_across_multiple_deal_id_pages(self, _timer):
        stream = DealInstallmentsStream()
        tap = MagicMock()
        tap.execute_request.side_effect = [
            _make_response({"success": True, "data": [{"id": 1}], "additional_data": {"next_cursor": "d-cursor-1"}}),
            _make_response({"success": True, "data": [{"id": 2}], "additional_data": {"next_cursor": None}}),
        ]

        batches = list(stream.get_child_ids(tap))

        self.assertEqual([[1], [2]], batches)
        # child_ids reflects only the most recently fetched page/batch.
        self.assertEqual([[2]], stream.child_ids)
        self.assertFalse(stream.more_ids_to_get)
        self.assertEqual(2, tap.execute_request.call_count)

        first_params = tap.execute_request.call_args_list[0].kwargs["params"]
        second_params = tap.execute_request.call_args_list[1].kwargs["params"]
        self.assertNotIn("cursor", first_params)
        self.assertEqual("d-cursor-1", second_params["cursor"])

    @patch("tap_pipedrive.streams.deal_installments.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_skips_empty_pages_without_yielding(self, _timer):
        stream = DealInstallmentsStream()
        tap = MagicMock()
        tap.execute_request.side_effect = [
            _make_response({"success": True, "data": [], "additional_data": {"next_cursor": "d-cursor-1"}}),
            _make_response({"success": True, "data": [{"id": 5}], "additional_data": {"next_cursor": None}}),
        ]

        batches = list(stream.get_child_ids(tap))

        self.assertEqual([[5]], batches)
        self.assertEqual([[5]], stream.child_ids)


class TestDealInstallmentsUpdateEndpointAndParams(unittest.TestCase):
    def test_get_name_returns_schema(self):
        stream = DealInstallmentsStream()
        self.assertEqual("deal_installments", stream.get_name())

    def test_update_endpoint_resets_cursor_and_sets_current_deal_ids(self):
        stream = DealInstallmentsStream()
        # Simulate a leftover cursor from the previous batch's installments pagination.
        stream.cursor = "leftover-installments-cursor"

        stream.update_endpoint([1, 2])

        self.assertIsNone(stream.cursor)
        self.assertEqual([1, 2], stream.current_deal_ids)
        # The endpoint is fixed (deal ids are sent as a query param, not part of the URL).
        self.assertEqual("deals/installments", stream.endpoint)

    def test_update_request_params_without_cursor_or_deal_ids(self):
        stream = DealInstallmentsStream()
        stream.cursor = None
        stream.current_deal_ids = None
        params = stream.update_request_params({})
        self.assertEqual({"limit": stream.limit}, params)

    def test_update_request_params_includes_deal_ids_as_comma_separated_string(self):
        stream = DealInstallmentsStream()
        stream.cursor = None
        stream.current_deal_ids = [1, 2, 3]
        params = stream.update_request_params({})
        self.assertEqual({"limit": stream.limit, "deal_ids": "1,2,3"}, params)

    def test_update_request_params_includes_cursor_when_present(self):
        stream = DealInstallmentsStream()
        stream.cursor = "installments-cursor"
        stream.current_deal_ids = [1]
        params = stream.update_request_params({})
        self.assertEqual(
            {"limit": stream.limit, "cursor": "installments-cursor", "deal_ids": "1"},
            params,
        )


class TestDealInstallmentsFullSync(unittest.TestCase):
    """
    Integration-style test driving tap.do_sync end to end with multiple
    pages of deal ids and multiple pages of installments per batch -
    verifying installments pagination correctly resets between batches.
    """

    def _execute_request_side_effect(self, endpoint, api_version, params=None):
        params = params or {}
        cursor = params.get("cursor")

        if endpoint == "deals":
            if cursor is None:
                return _make_response({
                    "success": True,
                    "data": [{"id": 1}, {"id": 2}],
                    "additional_data": {"next_cursor": "d-cursor-1"},
                })
            if cursor == "d-cursor-1":
                return _make_response({
                    "success": True,
                    "data": [{"id": 3}],
                    "additional_data": {"next_cursor": None},
                })

        elif endpoint == "deals/installments":
            deal_ids = params.get("deal_ids")

            if deal_ids == "1,2":
                if cursor is None:
                    return _make_response({
                        "success": True,
                        "data": [{"id": 100, "deal_id": 1, "amount": 10, "billing_date": "2025-01-01", "description": "first"}],
                        "additional_data": {"next_cursor": "i-cursor-1"},
                    })
                if cursor == "i-cursor-1":
                    return _make_response({
                        "success": True,
                        "data": [{"id": 101, "deal_id": 2, "amount": 20, "billing_date": "2025-02-01", "description": "second"}],
                        "additional_data": {"next_cursor": None},
                    })

            elif deal_ids == "3":
                # Must NOT inherit a leftover cursor from the previous batch's installments pagination.
                self.assertIsNone(cursor, "batch for deal 3 must not inherit a leftover installments cursor")
                return _make_response({
                    "success": True,
                    "data": [{"id": 200, "deal_id": 3, "amount": 30, "billing_date": "2025-03-01", "description": "third"}],
                    "additional_data": {"next_cursor": None},
                })

        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    def _build_catalog(self, stream):
        """
        Build a minimal, real Singer catalog for just this stream (mirrors
        what PipedriveTap.do_discover() would produce).
        """
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

    def test_do_sync_writes_installments_for_multiple_deal_batches_with_pagination(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {"bookmarks": {}})
        stream = DealInstallmentsStream()
        tap.streams = [stream]

        catalog = self._build_catalog(stream)

        with patch.object(tap, "execute_request", side_effect=self._execute_request_side_effect), \
             patch("tap_pipedrive.tap.PipedriveTap.get_selected_streams", return_value=["deal_installments"]), \
             patch("tap_pipedrive.tap.singer.write_record") as mock_write_record, \
             patch("tap_pipedrive.tap.singer.write_state"):
            tap.do_sync(catalog)

        written_records = [call.args[1] for call in mock_write_record.call_args_list]
        ids_by_deal = sorted((row["deal_id"], row["id"]) for row in written_records)

        self.assertEqual([(1, 100), (2, 101), (3, 200)], ids_by_deal)


class TestDealInstallmentsAccessDenied(unittest.TestCase):
    """
    Accounts without Growth-plan (or above) access get a 403 from Pipedrive
    for this endpoint. Verify that is handled the same way as any other
    inaccessible stream (PipedriveStream.check_access), rather than being
    treated as an implementation failure.
    """

    def test_check_access_includes_deal_ids(self):
        """
        /deals/installments requires deal_ids on every request. check_access
        runs before update_endpoint is ever called (current_deal_ids is None
        at that point), so the stream must inject a sentinel deal id itself
        or the probe request would be sent without deal_ids and fail with a
        400, breaking discovery instead of cleanly excluding the stream.
        """
        stream = DealInstallmentsStream()
        stream.current_deal_ids = None

        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.return_value = MagicMock()
        stream.tap = tap

        self.assertTrue(stream.check_access())

        call_kwargs = tap.execute_request.call_args.kwargs
        self.assertIn("deal_ids", call_kwargs["params"])
        self.assertEqual("0", call_kwargs["params"]["deal_ids"])

    def test_check_access_restores_current_deal_ids_after_probe(self):
        """The sentinel deal id used for probing must not leak into real syncs."""
        stream = DealInstallmentsStream()
        stream.current_deal_ids = None

        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.return_value = MagicMock()
        stream.tap = tap

        stream.check_access()

        self.assertIsNone(stream.current_deal_ids)

    def test_check_access_returns_true_on_bad_request_for_sentinel_deal_id(self):
        """A 400 for the sentinel deal id still means the endpoint is reachable."""
        stream = DealInstallmentsStream()
        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.side_effect = PipedriveBadRequestError("bad request")
        stream.tap = tap

        self.assertTrue(stream.check_access())

    def test_check_access_403_excludes_stream(self):
        stream = DealInstallmentsStream()
        tap = MagicMock()
        tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        tap.execute_request.side_effect = PipedriveForbiddenError(
            "HTTP-error-code: 403, Error: The company does not have access to this feature"
        )
        stream.tap = tap

        self.assertFalse(stream.check_access())

    def test_do_discover_excludes_deal_installments_on_403_but_keeps_other_streams(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {})
        currencies = CurrenciesStream()
        installments = DealInstallmentsStream()
        tap.streams = [currencies, installments]

        def execute_request_side_effect(endpoint, api_version, params=None):
            if endpoint == installments.endpoint:
                raise PipedriveForbiddenError(
                    "HTTP-error-code: 403, Error: The company does not have access to this feature"
                )
            return _make_response({"success": True, "data": [], "additional_data": {}})

        with patch.object(tap, "execute_request", side_effect=execute_request_side_effect):
            catalog = tap.do_discover()

        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertIn("currency", stream_ids)
        self.assertNotIn("deal_installments", stream_ids)
