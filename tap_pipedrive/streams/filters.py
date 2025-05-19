from tap_pipedrive.stream import PipedriveV1IncrementalStream


class FiltersStream(PipedriveV1IncrementalStream):
    endpoint = 'filters'
    schema = 'filters'
    key_properties = ['id', ]
    state_field = 'update_time'
    replication_method = 'INCREMENTAL'
