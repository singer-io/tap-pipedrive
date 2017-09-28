from tap_pipedrive.stream import PipedriveStream


class FiltersStream(PipedriveStream):
    endpoint = 'filters'
    key_properties = ['id', ]
    state_field = 'update_time'
