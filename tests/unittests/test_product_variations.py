import unittest
from unittest.mock import MagicMock, patch

from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_pipedrive.streams.product_variations import ProductVariationsStream
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


class TestProductVariationsGetChildIds(unittest.TestCase):
    """Unit tests for ProductVariationsStream.get_child_ids (parent/product pagination)."""

    @patch("tap_pipedrive.streams.product_variations.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_yields_multiple_products_from_a_single_page(self, _timer):
        stream = ProductVariationsStream()
        tap = MagicMock()
        tap.execute_request.return_value = _make_response({
            "success": True,
            "data": [{"id": 1}, {"id": 2}, {"id": 3}],
            "additional_data": {"next_cursor": None},
        })

        ids = list(stream.get_child_ids(tap))

        self.assertEqual([1, 2, 3], ids)
        self.assertEqual([1, 2, 3], stream.child_ids)
        self.assertFalse(stream.more_ids_to_get)
        self.assertIsNotNone(stream.stream_start)
        # Parent (products) pagination must not touch the child (variations) cursor.
        self.assertIsNone(stream.cursor)

        call_kwargs = tap.execute_request.call_args.kwargs
        self.assertEqual("products", tap.execute_request.call_args.args[0])
        self.assertEqual("api/v2", call_kwargs["api_version"])
        self.assertNotIn("cursor", call_kwargs["params"])

    @patch("tap_pipedrive.streams.product_variations.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_paginates_across_multiple_product_pages(self, _timer):
        stream = ProductVariationsStream()
        tap = MagicMock()
        tap.execute_request.side_effect = [
            _make_response({"success": True, "data": [{"id": 1}], "additional_data": {"next_cursor": "p-cursor-1"}}),
            _make_response({"success": True, "data": [{"id": 2}], "additional_data": {"next_cursor": None}}),
        ]

        ids = list(stream.get_child_ids(tap))

        self.assertEqual([1, 2], ids)
        # child_ids reflects only the most recently fetched page.
        self.assertEqual([2], stream.child_ids)
        self.assertFalse(stream.more_ids_to_get)
        self.assertEqual(2, tap.execute_request.call_count)

        first_params = tap.execute_request.call_args_list[0].kwargs["params"]
        second_params = tap.execute_request.call_args_list[1].kwargs["params"]
        self.assertNotIn("cursor", first_params)
        self.assertEqual("p-cursor-1", second_params["cursor"])

    @patch("tap_pipedrive.streams.product_variations.singer.metrics.http_request_timer", return_value=_DummyTimer())
    def test_skips_empty_pages_without_yielding(self, _timer):
        stream = ProductVariationsStream()
        tap = MagicMock()
        tap.execute_request.side_effect = [
            _make_response({"success": True, "data": [], "additional_data": {"next_cursor": "p-cursor-1"}}),
            _make_response({"success": True, "data": [{"id": 5}], "additional_data": {"next_cursor": None}}),
        ]

        ids = list(stream.get_child_ids(tap))

        self.assertEqual([5], ids)
        self.assertEqual([5], stream.child_ids)


class TestProductVariationsUpdateEndpoint(unittest.TestCase):
    """Verify variation pagination resets for every product."""

    def test_get_name_returns_schema(self):
        stream = ProductVariationsStream()
        self.assertEqual("product_variations", stream.get_name())

    def test_update_endpoint_resets_cursor_and_sets_endpoint(self):
        stream = ProductVariationsStream()
        # Simulate a leftover cursor from the previous product's variation pagination.
        stream.cursor = "leftover-variation-cursor"

        stream.update_endpoint(2)

        self.assertIsNone(stream.cursor)
        self.assertEqual("products/2/variations", stream.endpoint)
        self.assertEqual(2, stream.current_product_id)

    def test_update_request_params_only_includes_limit_and_cursor(self):
        stream = ProductVariationsStream()
        stream.cursor = None
        params = stream.update_request_params({})
        self.assertEqual({"limit": stream.limit}, params)

        stream.cursor = "variation-cursor"
        params = stream.update_request_params({})
        self.assertEqual({"limit": stream.limit, "cursor": "variation-cursor"}, params)


class TestProductVariationsFullSync(unittest.TestCase):
    """
    Integration-style test driving tap.do_sync end to end with multiple
    products, multiple pages of products, and multiple pages of variations
    per product - verifying pagination correctly resets between products.
    """

    def _execute_request_side_effect(self, endpoint, api_version, params=None):
        params = params or {}
        cursor = params.get("cursor")

        if endpoint == "products":
            if cursor is None:
                return _make_response({
                    "success": True,
                    "data": [{"id": 1}],
                    "additional_data": {"next_cursor": "p-cursor-1"},
                })
            if cursor == "p-cursor-1":
                return _make_response({
                    "success": True,
                    "data": [{"id": 2}],
                    "additional_data": {"next_cursor": None},
                })

        elif endpoint == "products/1/variations":
            if cursor is None:
                return _make_response({
                    "success": True,
                    "data": [{"id": 10, "name": "V10", "product_id": 1, "prices": []}],
                    "additional_data": {"next_cursor": "v-cursor-1"},
                })
            if cursor == "v-cursor-1":
                return _make_response({
                    "success": True,
                    "data": [{"id": 11, "name": "V11", "product_id": 1, "prices": []}],
                    "additional_data": {"next_cursor": None},
                })

        elif endpoint == "products/2/variations":
            # Must NOT inherit a leftover cursor from product 1's variation pagination.
            self.assertIsNone(cursor, "product 2's first variations request must not carry a leftover cursor")
            return _make_response({
                "success": True,
                "data": [{"id": 20, "name": "V20", "product_id": 2, "prices": []}],
                "additional_data": {"next_cursor": None},
            })

        raise AssertionError(f"Unexpected request: endpoint={endpoint}, params={params}")

    def _build_catalog(self, stream):
        """
        Build a minimal, real Singer catalog for just this stream (mirrors
        what PipedriveTap.do_discover() would produce), without needing the
        parent 'products' stream present in tap.streams for pruning.
        """
        schema_dict = stream.get_schema()
        schema = Schema.from_dict(schema_dict)
        meta = metadata.get_standard_metadata(
            schema=schema_dict,
            key_properties=stream.key_properties,
            replication_method=stream.replication_method,
        )
        meta = metadata.to_map(meta)
        meta = metadata.write(meta, (), "parent-tap-stream-id", stream.parent)
        meta = metadata.to_list(meta)

        return Catalog([CatalogEntry(
            stream=stream.schema,
            tap_stream_id=stream.schema,
            key_properties=stream.key_properties,
            schema=schema,
            metadata=meta,
        )])

    def test_do_sync_writes_variations_for_multiple_products_with_pagination(self):
        tap = PipedriveTap({"api_token": "x", "start_date": "2024-01-01T00:00:00Z"}, {"bookmarks": {}})
        stream = ProductVariationsStream()
        tap.streams = [stream]

        catalog = self._build_catalog(stream)

        with patch.object(tap, "execute_request", side_effect=self._execute_request_side_effect), \
             patch("tap_pipedrive.tap.PipedriveTap.get_selected_streams", return_value=["product_variations"]), \
             patch("tap_pipedrive.tap.singer.write_record") as mock_write_record, \
             patch("tap_pipedrive.tap.singer.write_state"):
            tap.do_sync(catalog)

        written_records = [call.args[1] for call in mock_write_record.call_args_list]
        ids_by_product = sorted((row["product_id"], row["id"]) for row in written_records)

        self.assertEqual([(1, 10), (1, 11), (2, 20)], ids_by_product)
