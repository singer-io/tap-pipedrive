from tap_pipedrive.stream import PipedriveStream


class FiltersStream(PipedriveStream):
    endpoint = 'filters'
    key_properties = ['id', ]
