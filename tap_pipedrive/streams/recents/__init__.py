import singer
from tap_pipedrive.stream import PipedriveStream

logger = singer.get_logger()


class RecentsStream(PipedriveStream):
    endpoint = "recents"
    items = None
    schema = None
    schema_path = "schemas/recents/{}.json"
    replication_method = "INCREMENTAL"

    def update_request_params(self, params):
        """
        Filter recents enpoint data with items and since_timestamp
        """
        params.update(
            {
                "since_timestamp": self.initial_state.subtract(
                    seconds=1
                ).to_datetime_string(),
                "items": self.items,
            }
        )
        return params

    def write_schema(self):
        # for /recents/ streams override default (schema name equals to endpoint) with items
        schema_to_write = self.post_process_schema()
        singer.write_schema(
            self.schema, schema_to_write, key_properties=self.key_properties
        )

    def post_process_schema(self):
        return self.get_schema()

    def get_name(self):
        return self.schema

    def process_row(self, row):
        return row["data"]
