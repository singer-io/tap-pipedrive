from tap_pipedrive.stream import PipedriveStream

class ActivitiesStream(PipedriveStream):
    endpoint = 'activities'
    schema = 'activities'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
