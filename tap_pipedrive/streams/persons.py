from tap_pipedrive.stream import PipedriveStream

class PersonsStream(PipedriveStream):
    endpoint = 'persons'
    schema = 'persons'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
