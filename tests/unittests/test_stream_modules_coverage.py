import unittest

from tap_pipedrive.streams.deal_fields import DealFields
from tap_pipedrive.streams.deal_products import DealsProductsStream
from tap_pipedrive.streams.dealflow import DealStageChangeStream
from tap_pipedrive.streams.deals import DealsStream
from tap_pipedrive.streams.notes import NotesStream


class TestStreamModulesCoverage(unittest.TestCase):
    def test_deal_fields_get_name_and_process_row(self):
        stream = DealFields()
        self.assertEqual("deal_fields", stream.get_name())
        self.assertIsNone(stream.process_row({"parent_id": 99, "id": 1}))
        self.assertEqual({"id": 1}, stream.process_row({"id": 1}))

    def test_deal_products_get_name_and_update_endpoint(self):
        stream = DealsProductsStream()
        self.assertEqual("deal_products", stream.get_name())
        stream.update_endpoint(123)
        self.assertEqual("deals/123/products", stream.endpoint)

    def test_dealflow_get_name_process_and_update_endpoint(self):
        stream = DealStageChangeStream()
        self.assertEqual("dealflow", stream.get_name())
        self.assertEqual(
            {"field_key": "add_time", "value": "x"},
            stream.process_row({"object": "dealChange", "data": {"field_key": "add_time", "value": "x"}}),
        )
        self.assertIsNone(stream.process_row({"object": "dealChange", "data": {"field_key": "title"}}))
        self.assertIsNone(stream.process_row({"object": "other", "data": {"field_key": "add_time"}}))
        stream.update_endpoint(45)
        self.assertEqual("deals/45/flow", stream.endpoint)

    def test_deals_update_request_params_sets_status(self):
        stream = DealsStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        params = stream.update_request_params({})
        self.assertEqual("open,won,lost,deleted", params["status"])

    def test_notes_update_request_params_sets_sort(self):
        stream = NotesStream()
        params = stream.update_request_params({})
        self.assertEqual("update_time desc", params["sort"])

