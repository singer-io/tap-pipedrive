from tap_pipedrive.stream import PipedriveV1IncrementalStream

class UsersStream(PipedriveV1IncrementalStream):
    endpoint = 'users'
    schema = 'users'
    api_version = 'v1'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'modified'
