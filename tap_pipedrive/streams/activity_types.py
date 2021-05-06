from tap_pipedrive.stream import PipedriveStream


class ActivityTypesStream(PipedriveStream):
    endpoint = 'activityTypes'
    schema = 'activity_types'
    key_properties = ['id', ]
    # As it has no meaning to have
    # state_field = 'update_time'
