from tap_pipedrive.stream import PipedriveStream


class PipelinesStream(PipedriveStream):
    endpoint = 'pipelines'
    schema = 'pipelines'
    key_properties = ['id', ]
    state_field = 'update_time'
