import singer
from tap_pipedrive.streams.recents import RecentsStream


class RecentDealsStream(RecentsStream):
    items = 'deal'
    schema = 'deals'
    key_properties = ['id', ]

    def write_schema(self):
        # override schema to avoid duplicity with deals from /deals/ endpoint
        singer.write_schema('recents-{}'.format(self.schema), self.get_schema(), key_properties=self.key_properties)

    def write_record(self, row):
        # override schema to avoid duplicity with deals from /deals/ endpoint
        singer.write_record('recents-{}'.format(self.schema), row)
