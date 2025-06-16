from tap_pipedrive.stream import DynamicSchemaStream

class OrganizationsStream(DynamicSchemaStream):
    endpoint = 'organizations'
    fields_endpoint = 'organizationFields'
    schema = 'organizations'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
