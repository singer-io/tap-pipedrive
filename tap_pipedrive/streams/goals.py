from tap_pipedrive.stream import PipedriveStream


class GoalsStream(PipedriveStream):
    endpoint = 'goals'
    schema = 'goals'
    key_properties = ['id', ]
