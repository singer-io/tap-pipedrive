import os
from singer import utils
from tap_pipedrive.stream import PipedriveStream


class RecentsStream(PipedriveStream):
    endpoint = 'recents'
    items = None
    schema = None
    since_timestamp = '1970-01-01 00:00:00'

    def get_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))),
                                   'schemas/recents/{}.json'.format(self.schema))
        schema = utils.load_json(schema_path)
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
