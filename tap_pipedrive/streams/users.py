from tap_pipedrive.stream import PipedriveV1IncrementalStream

class UsersStream(PipedriveV1IncrementalStream):
    endpoint = 'users'
    schema = 'users'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'modified'
