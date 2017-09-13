from tap_pipedrive.stream import PipedriveStream


class GoalsStream(PipedriveStream):
    endpoint = 'goals'
    key_properties = ['id', ]
