from tap_pipedrive.stream import PipedriveStream

class OrganizationsStream(PipedriveStream):
    endpoint = 'organizations'
    schema = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
