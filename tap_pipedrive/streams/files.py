from tap_pipedrive.stream import RecentsStream

class FilesStream(RecentsStream):
    endpoint = 'files'
    schema = 'files'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    items = 'file'
