import os
import singer
from tap_pipedrive.stream import PipedriveStream

logger = singer.get_logger()


class RecentsStream(PipedriveStream):
    endpoint = 'recents'
    items = None
    schema = None
    since_timestamp = '1970-01-01 00:00:00'

    def get_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))),
                                   'schemas/recents/{}.json'.format(self.schema))
        schema = singer.utils.load_json(schema_path)
        return schema

    def update_request_params(self, params):
        """
        Filter recents enpoint data with items and since_timestamp
        """
        params.update({
            'since_timestamp': self.since_timestamp,
            'items': self.items
        })
        return params

    def write_schema(self):
        # for /recents/ streams override default (schema name equals to endpoint) with items
        singer.write_schema(self.schema, self.get_schema(), key_properties=self.key_properties)

    def write_record(self, row):
        singer.write_record(self.schema, row)

    def get_name(self):
        return "recents [item={}]".format(self.items)
