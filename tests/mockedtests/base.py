"""Base helpers for tap-pipedrive mocked integration tests."""

from singer import metadata


class MockResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeStream:
    endpoint = "deals"
    api_version = "api/v2"
    limit = 100
    id_list = False

    def __init__(self, schema="deals", state_field="update_time", replication_method="INCREMENTAL"):
        self.schema = schema
        self.endpoint = schema
        self.state_field = state_field
        self.replication_method = replication_method
        self.key_properties = ["id"]
        self.more_items_in_collection = True
        self.initial_state = None
        self.earliest_state = None
        self.start = 0
        self.cursor = None

    def get_schema(self):
        properties = {
            "id": {"type": ["null", "integer"]},
            "name": {"type": ["null", "string"]},
        }
        if self.state_field:
            properties[self.state_field] = {"type": ["null", "string"], "format": "date-time"}
        return {"type": "object", "properties": properties}

    def set_initial_state(self, state, start_date):
        if self.state_field:
            bookmark = state.get("bookmarks", {}).get(self.schema, {}).get(self.state_field)
            self.initial_state = bookmark or start_date
            self.earliest_state = self.initial_state
        else:
            self.initial_state = start_date
            self.earliest_state = start_date

    def write_schema(self):
        return None

    def has_data(self):
        return self.more_items_in_collection

    def paginate(self, response):
        pagination = response.json().get("additional_data", {}).get("pagination", {})
        self.more_items_in_collection = pagination.get("more_items_in_collection", False)
        if "next_start" in pagination:
            self.start = pagination["next_start"]

    def process_row(self, row):
        return row

    def write_record(self, row):
        if not self.state_field:
            return True
        value = row.get(self.state_field)
        return (not value) or value >= self.initial_state

    def update_state(self, row):
        if not self.state_field:
            return
        value = row.get(self.state_field)
        if value and value >= self.earliest_state:
            self.earliest_state = value


class PipedriveMockedBaseTest:
    @staticmethod
    def base_config(start_date="2024-01-01T00:00:00Z"):
        return {
            "api_token": "mock-token",
            "start_date": start_date,
            "user-agent": "tap-pipedrive <tests@example.com>",
        }

    @staticmethod
    def select_all_streams(catalog):
        for stream in catalog.streams:
            mdata = metadata.to_map(stream.metadata)
            mdata[()] = dict(mdata.get((), {}))
            mdata[()]["selected"] = True
            stream.metadata = metadata.to_list(mdata)
        return catalog
