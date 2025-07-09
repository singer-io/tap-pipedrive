from tap_pipedrive.stream import DynamicSchemaStream

class DealsStream(DynamicSchemaStream):
    endpoint = 'deals'
    fields_endpoint = 'dealFields'
    schema = 'deals'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None

