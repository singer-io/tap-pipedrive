import os
from singer import utils


class Stream(object):
    endpoint = ''
    key_properties = []

    def get_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                   'schemas/{}.json'.format(self.endpoint))
        schema = utils.load_json(schema_path)
        return schema
