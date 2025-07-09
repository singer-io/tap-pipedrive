from tap_pipedrive.stream import RecentsStream

class UsersStream(RecentsStream):
    endpoint = 'users'
    schema = 'users'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'modified'
    items = 'user'
