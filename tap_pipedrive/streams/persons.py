from tap_pipedrive.stream import DynamicSchemaStream

class PersonsStream(DynamicSchemaStream):
    endpoint = 'persons'
    fields_endpoint = 'personFields'
    schema = 'persons'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
