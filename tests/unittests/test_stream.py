"""
Unit tests for tap_pipedrive.stream module.

Covers: PipedriveStream, PipedriveV1IncrementalStream, RecentsStream,
        PipedriveIncrementalStreamUsingSort, PipedriveIterStream.find_deal_ids
"""
import unittest
from unittest.mock import MagicMock, patch

import pendulum

from tap_pipedrive.stream import (
    PipedriveStream,
    PipedriveV1IncrementalStream,
    RecentsStream,
    PipedriveIncrementalStreamUsingSort,
    PipedriveIterStream,
)


# ---------------------------------------------------------------------------
# Minimal concrete subclasses (PipedriveStream is not abstract, but its schema
# and state_field must be set to test most methods meaningfully)
# ---------------------------------------------------------------------------

class _SimpleStream(PipedriveStream):
    schema = "test_stream"
    state_field = "update_time"
    key_properties = ["id"]
    schema_path = "schemas/deals.json"  # reuse any real schema to avoid file errors


class _SimpleV1Stream(PipedriveV1IncrementalStream):
    schema = "test_v1_stream"
    state_field = "update_time"
    key_properties = ["id"]
    schema_path = "schemas/activities.json"


class _SimpleIncrementalSortStream(PipedriveIncrementalStreamUsingSort):
    schema = "test_sort_stream"
    state_field = "update_time"
    key_properties = ["id"]
    schema_path = "schemas/deals.json"


class _SimpleIterStream(PipedriveIterStream):
    schema = "dealflow"
    state_field = "log_time"
    key_properties = ["id"]
    base_endpoint = "deals"
    schema_path = "schemas/dealflow.json"


class _SimpleRecentsStream(RecentsStream):
    schema = "activities"
    state_field = "update_time"
    key_properties = ["id"]
    items = "activity"
    schema_path = "schemas/activities.json"
    recent_endpoint = "recents"
    endpoint = "activities"


# ---------------------------------------------------------------------------
# TestPipedriveStreamHasData
# ---------------------------------------------------------------------------

class TestPipedriveStreamHasData(unittest.TestCase):
    """Verify has_data delegates to more_items_in_collection."""

    def test_has_data_true_when_more_items(self):
        """has_data returns True when more_items_in_collection is True."""
        stream = _SimpleStream()
        stream.more_items_in_collection = True
        self.assertTrue(stream.has_data())

    def test_has_data_false_when_no_more_items(self):
        """has_data returns False when more_items_in_collection is False."""
        stream = _SimpleStream()
        stream.more_items_in_collection = False
        self.assertFalse(stream.has_data())


# ---------------------------------------------------------------------------
# TestPipedriveStreamSetInitialState
# ---------------------------------------------------------------------------

class TestPipedriveStreamSetInitialState(unittest.TestCase):
    """Verify set_initial_state reads bookmark or falls back to start_date."""

    def test_uses_bookmark_when_present(self):
        """set_initial_state uses the bookmark value from state when available."""
        stream = _SimpleStream()
        state = {"bookmarks": {"test_stream": {"update_time": "2024-06-01T00:00:00Z"}}}
        stream.set_initial_state(state, "2024-01-01T00:00:00Z")
        self.assertEqual(stream.initial_state, "2024-06-01T00:00:00Z")
        self.assertEqual(stream.earliest_state, "2024-06-01T00:00:00Z")

    def test_falls_back_to_start_date_when_no_bookmark(self):
        """set_initial_state falls back to start_date when no bookmark exists."""
        stream = _SimpleStream()
        state = {"bookmarks": {}}
        stream.set_initial_state(state, "2024-01-01T00:00:00Z")
        self.assertEqual(stream.initial_state, "2024-01-01T00:00:00Z")

    def test_falls_back_to_start_date_when_state_empty(self):
        """set_initial_state falls back to start_date when state is empty."""
        stream = _SimpleStream()
        stream.set_initial_state({}, "2024-01-01T00:00:00Z")
        self.assertEqual(stream.initial_state, "2024-01-01T00:00:00Z")


# ---------------------------------------------------------------------------
# TestPipedriveStreamUpdateState
# ---------------------------------------------------------------------------

class TestPipedriveStreamUpdateState(unittest.TestCase):
    """Verify update_state advances earliest_state when a newer record arrives."""

    def test_updates_earliest_state_when_row_is_newer(self):
        """update_state advances earliest_state when the record's timestamp is newer."""
        stream = _SimpleStream()
        stream.earliest_state = "2024-01-01T00:00:00Z"
        row = {"update_time": "2024-06-01T00:00:00.000000Z"}
        stream.update_state(row)
        self.assertEqual(stream.earliest_state, "2024-06-01T00:00:00Z")

    def test_does_not_update_when_row_is_older(self):
        """update_state does not change earliest_state when the record is older."""
        stream = _SimpleStream()
        stream.earliest_state = "2024-06-01T00:00:00Z"
        row = {"update_time": "2024-01-01T00:00:00.000000Z"}
        stream.update_state(row)
        self.assertEqual(stream.earliest_state, "2024-06-01T00:00:00Z")

    def test_does_not_update_when_state_field_missing(self):
        """update_state does not raise and leaves earliest_state unchanged when field is absent."""
        stream = _SimpleStream()
        stream.earliest_state = "2024-01-01T00:00:00Z"
        row = {}
        stream.update_state(row)
        self.assertEqual(stream.earliest_state, "2024-01-01T00:00:00Z")


# ---------------------------------------------------------------------------
# TestPipedriveStreamWriteRecord
# ---------------------------------------------------------------------------

class TestPipedriveStreamWriteRecord(unittest.TestCase):
    """Verify write_record filters records based on initial_state."""

    def test_returns_true_when_no_state_field_in_row(self):
        """write_record returns True when the row does not contain the state_field."""
        stream = _SimpleStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        self.assertTrue(stream.write_record({}))

    def test_returns_true_when_record_is_at_or_after_initial_state(self):
        """write_record returns True when record's replication key >= initial_state."""
        stream = _SimpleStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        row = {"update_time": "2024-06-01T00:00:00Z"}
        self.assertTrue(stream.write_record(row))

    def test_returns_false_when_record_is_before_initial_state(self):
        """write_record returns False when record's replication key < initial_state."""
        stream = _SimpleStream()
        stream.initial_state = "2024-06-01T00:00:00Z"
        row = {"update_time": "2024-01-01T00:00:00Z"}
        self.assertFalse(stream.write_record(row))


# ---------------------------------------------------------------------------
# TestPipedriveStreamPaginate (cursor-based)
# ---------------------------------------------------------------------------

class TestPipedriveStreamPaginate(unittest.TestCase):
    """Verify cursor-based paginate correctly advances the cursor."""

    def _make_response(self, next_cursor=None):
        payload = {"additional_data": {}}
        if next_cursor:
            payload["additional_data"]["next_cursor"] = next_cursor
        resp = MagicMock()
        resp.json.return_value = payload
        return resp

    def test_sets_cursor_and_more_items_when_next_cursor_present(self):
        """paginate sets cursor and more_items_in_collection=True when next_cursor is returned."""
        stream = _SimpleStream()
        stream.more_items_in_collection = False
        resp = self._make_response(next_cursor="abc123")
        stream.paginate(resp)
        self.assertEqual(stream.cursor, "abc123")
        self.assertTrue(stream.more_items_in_collection)

    def test_clears_more_items_when_no_next_cursor(self):
        """paginate sets more_items_in_collection=False when no next_cursor is returned."""
        stream = _SimpleStream()
        stream.more_items_in_collection = True
        resp = self._make_response(next_cursor=None)
        stream.paginate(resp)
        self.assertFalse(stream.more_items_in_collection)


# ---------------------------------------------------------------------------
# TestPipedriveV1IncrementalStreamPaginate (page-based)
# ---------------------------------------------------------------------------

class TestPipedriveV1IncrementalStreamPaginate(unittest.TestCase):
    """Verify V1 page-based paginate advances start offset correctly."""

    def _make_response(self, more_items, next_start=None):
        pagination = {"more_items_in_collection": more_items}
        if next_start is not None:
            pagination["next_start"] = next_start
        payload = {"additional_data": {"pagination": pagination}}
        resp = MagicMock()
        resp.json.return_value = payload
        return resp

    def test_advances_start_when_more_items_present(self):
        """V1 paginate advances start offset when more pages are available."""
        stream = _SimpleV1Stream()
        resp = self._make_response(more_items=True, next_start=100)
        stream.paginate(resp)
        self.assertTrue(stream.more_items_in_collection)
        self.assertEqual(stream.start, 100)

    def test_stops_when_no_more_items(self):
        """V1 paginate sets more_items_in_collection=False when no more pages exist."""
        stream = _SimpleV1Stream()
        stream.more_items_in_collection = True
        resp = self._make_response(more_items=False)
        stream.paginate(resp)
        self.assertFalse(stream.more_items_in_collection)

    def test_stops_when_no_pagination_key_in_additional_data(self):
        """V1 paginate stops when 'pagination' key is absent from additional_data."""
        stream = _SimpleV1Stream()
        stream.more_items_in_collection = True
        resp = MagicMock()
        resp.json.return_value = {"additional_data": {}}
        stream.paginate(resp)
        self.assertFalse(stream.more_items_in_collection)


# ---------------------------------------------------------------------------
# TestRecentsStreamUpdateRequestParams
# ---------------------------------------------------------------------------

class TestRecentsStreamUpdateRequestParams(unittest.TestCase):
    """Verify RecentsStream switches to /recents endpoint for recent data."""

    def test_uses_recents_endpoint_when_initial_state_within_one_month(self):
        """update_request_params uses recents endpoint when initial_state is within the last month."""
        stream = _SimpleRecentsStream()
        recent_date = pendulum.now().subtract(days=7).strftime("%Y-%m-%dT%H:%M:%SZ")
        stream.initial_state = recent_date
        stream.cursor = None
        params = stream.update_request_params({})
        self.assertEqual(stream.endpoint, "recents")
        self.assertIn("since_timestamp", params)
        self.assertIn("items", params)

    def test_uses_base_endpoint_when_initial_state_older_than_one_month(self):
        """update_request_params falls back to the base endpoint for old initial_state."""
        stream = _SimpleRecentsStream()
        old_date = pendulum.now().subtract(months=3).strftime("%Y-%m-%dT%H:%M:%SZ")
        stream.initial_state = old_date
        stream.cursor = None
        # endpoint should NOT be switched to recents
        stream.endpoint = "activities"  # reset
        params = stream.update_request_params({})
        self.assertNotEqual(stream.endpoint, "recents")


# ---------------------------------------------------------------------------
# TestRecentsStreamProcessRow
# ---------------------------------------------------------------------------

class TestRecentsStreamProcessRow(unittest.TestCase):
    """Verify RecentsStream.process_row unwraps /recents nested data correctly."""

    def test_process_row_on_recents_endpoint_returns_data_dict(self):
        """process_row returns the nested 'data' dict when endpoint is 'recents'."""
        stream = _SimpleRecentsStream()
        stream.endpoint = "recents"
        row = {"data": {"id": 1, "name": "Test"}, "object": "activity"}
        result = stream.process_row(row)
        self.assertEqual(result, {"id": 1, "name": "Test"})

    def test_process_row_on_recents_endpoint_with_list_data_returns_first(self):
        """process_row returns the first element when 'data' is a list."""
        stream = _SimpleRecentsStream()
        stream.endpoint = "recents"
        row = {"data": [{"id": 2}, {"id": 3}], "object": "activity"}
        result = stream.process_row(row)
        self.assertEqual(result, {"id": 2})

    def test_process_row_on_base_endpoint_returns_row_as_is(self):
        """process_row returns the row unchanged when not using the recents endpoint."""
        stream = _SimpleRecentsStream()
        stream.endpoint = "activities"
        row = {"id": 5, "name": "Direct"}
        result = stream.process_row(row)
        self.assertEqual(result, row)


# ---------------------------------------------------------------------------
# TestPipedriveIncrementalStreamUsingSortWriteRecord
# ---------------------------------------------------------------------------

class TestPipedriveIncrementalStreamUsingSortWriteRecord(unittest.TestCase):
    """Verify descending-sort stream stops fetching once records go below bookmark."""

    def test_returns_true_for_records_at_or_after_initial_state(self):
        """write_record returns True when replication key >= initial_state."""
        stream = _SimpleIncrementalSortStream()
        stream.initial_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = True
        row = {"update_time": "2024-06-01T00:00:00Z"}
        self.assertTrue(stream.write_record(row))
        self.assertTrue(stream.more_items_in_collection)

    def test_returns_false_and_stops_pagination_for_old_records(self):
        """write_record returns False and sets more_items_in_collection=False for old records."""
        stream = _SimpleIncrementalSortStream()
        stream.initial_state = "2024-06-01T00:00:00Z"
        stream.more_items_in_collection = True
        row = {"update_time": "2024-01-01T00:00:00Z"}
        result = stream.write_record(row)
        self.assertFalse(result)
        self.assertFalse(stream.more_items_in_collection)


# ---------------------------------------------------------------------------
# TestPipedriveIncrementalStreamUsingSortUpdateState
# ---------------------------------------------------------------------------

class TestPipedriveIncrementalStreamUsingSortUpdateState(unittest.TestCase):
    """Verify descending-sort stream only updates state after all pages are consumed."""

    def test_does_not_update_state_while_more_items_remain(self):
        """update_state does not advance earliest_state while more_items_in_collection is True."""
        stream = _SimpleIncrementalSortStream()
        stream.earliest_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = True
        row = {"update_time": "2024-06-01T00:00:00.000000Z"}
        stream.update_state(row)
        self.assertEqual(stream.earliest_state, "2024-01-01T00:00:00Z")

    def test_updates_state_when_no_more_items(self):
        """update_state advances earliest_state once all pages are fetched."""
        stream = _SimpleIncrementalSortStream()
        stream.earliest_state = "2024-01-01T00:00:00Z"
        stream.more_items_in_collection = False
        row = {"update_time": "2024-06-01T00:00:00.000000Z"}
        stream.update_state(row)
        self.assertEqual(stream.earliest_state, "2024-06-01T00:00:00Z")


# ---------------------------------------------------------------------------
# TestPipedriveIterStreamFindDealIds
# ---------------------------------------------------------------------------

class TestPipedriveIterStreamFindDealIds(unittest.TestCase):
    """Verify find_deal_ids correctly identifies added and stage-changed deals."""

    def _stream(self):
        stream = _SimpleIterStream()
        return stream

    def _record(self, deal_id, add_time, stage_change_time=None):
        return {
            "id": deal_id,
            "add_time": add_time,
            "stage_change_time": stage_change_time,
            "update_time": add_time,
        }

    def test_returns_deal_id_when_added_within_window(self):
        """find_deal_ids includes deals added within the start/stop window."""
        stream = self._stream()
        data = [self._record(1, "2024-03-01T00:00:00Z")]
        result = stream.find_deal_ids(data, start="2024-01-01T00:00:00Z", stop="2024-06-01T00:00:00Z")
        self.assertIn(1, result)

    def test_excludes_deal_added_before_window(self):
        """find_deal_ids excludes deals added before the start of the window."""
        stream = self._stream()
        data = [self._record(2, "2023-01-01T00:00:00Z")]
        result = stream.find_deal_ids(data, start="2024-01-01T00:00:00Z", stop="2024-06-01T00:00:00Z")
        self.assertNotIn(2, result)

    def test_includes_deal_with_stage_change_within_window(self):
        """find_deal_ids includes deals with a stage_change_time within the window."""
        stream = self._stream()
        data = [self._record(3, "2023-01-01T00:00:00Z", stage_change_time="2024-03-01T00:00:00Z")]
        result = stream.find_deal_ids(data, start="2024-01-01T00:00:00Z", stop="2024-06-01T00:00:00Z")
        self.assertIn(3, result)

    def test_excludes_deal_with_stage_change_outside_window(self):
        """find_deal_ids excludes deals whose stage_change_time falls outside the window."""
        stream = self._stream()
        data = [self._record(4, "2023-01-01T00:00:00Z", stage_change_time="2023-06-01T00:00:00Z")]
        result = stream.find_deal_ids(data, start="2024-01-01T00:00:00Z", stop="2024-06-01T00:00:00Z")
        self.assertNotIn(4, result)

    def test_no_duplicates_when_deal_added_and_has_stage_change(self):
        """find_deal_ids does not duplicate a deal that both was added and had a stage change."""
        stream = self._stream()
        data = [self._record(5, "2024-03-01T00:00:00Z", stage_change_time="2024-04-01T00:00:00Z")]
        result = stream.find_deal_ids(data, start="2024-01-01T00:00:00Z", stop="2024-06-01T00:00:00Z")
        self.assertEqual(result.count(5), 1)

    def test_empty_data_returns_empty_list(self):
        """find_deal_ids returns an empty list when given no records."""
        stream = self._stream()
        result = stream.find_deal_ids([], start="2024-01-01T00:00:00Z", stop="2024-06-01T00:00:00Z")
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
