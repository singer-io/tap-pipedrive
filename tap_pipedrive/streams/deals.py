from tap_pipedrive.stream import PipedriveStream


class DealsStream(PipedriveStream):
    endpoint = 'deals'
    key_properties = ['id', ]
