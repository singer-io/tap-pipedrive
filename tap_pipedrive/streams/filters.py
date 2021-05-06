from tap_pipedrive.stream import PipedriveStream


class FiltersStream(PipedriveStream):
    endpoint = 'filters'
    schema = 'filters'
    key_properties = ['id', ]
    # As it has no meaning to have
    # state_field = 'update_time'
