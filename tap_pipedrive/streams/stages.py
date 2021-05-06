from tap_pipedrive.stream import PipedriveStream


class StagesStream(PipedriveStream):
    endpoint = 'stages'
    schema = 'stages'
    key_properties = ['id', ]
    # As it has no meaning to have
    # state_field = 'update_time'
