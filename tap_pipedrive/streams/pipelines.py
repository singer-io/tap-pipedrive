from tap_pipedrive.stream import PipedriveStream


class PipelinesStream(PipedriveStream):
    endpoint = 'pipelines'
    key_properties = ['id', ]
    state_field = 'update_time'
