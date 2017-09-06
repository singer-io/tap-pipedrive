from tap_pipedrive.singer.stream import Stream


class DealsStream(Stream):
    endpoint = 'deals'
    key_properties = ['id', ]
