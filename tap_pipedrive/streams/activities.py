from tap_pipedrive.stream import DynamicSchemaStream

class ActivitiesStream(DynamicSchemaStream):
    endpoint = 'activities'
    fields_endpoint = 'activityFields'
    schema = 'activities'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
