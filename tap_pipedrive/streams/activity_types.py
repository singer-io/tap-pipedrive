from tap_pipedrive.stream import PipedriveV1IncrementalStream


class ActivityTypesStream(PipedriveV1IncrementalStream):
    endpoint = 'activityTypes'
    schema = 'activity_types'
    key_properties = ['id', ]
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
