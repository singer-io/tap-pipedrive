from tap_pipedrive.stream import PipedriveStream

class DealsStream(PipedriveStream):
    endpoint = 'deals'
    schema = 'deals'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    api_version = 'api/v2'
    cursor = None

