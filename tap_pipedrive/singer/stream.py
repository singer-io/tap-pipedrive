import os
import singer


class Stream(object):
    endpoint = ''
    key_properties = []

    def get_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                   'schemas/{}.json'.format(self.endpoint))
        schema = singer.utils.load_json(schema_path)
        return schema

    def has_data(self):
        raise NotImplementedError("Implement this method")

    def paginate(self, response):
        raise NotImplementedError("Implement this method")

    def write_schema(self):
        singer.write_schema(self.endpoint, self.get_schema(), key_properties=self.key_properties)

    def write_record(self, row):
        singer.write_record(self.endpoint, row)

    def get_name(self):
        return self.endpoint
