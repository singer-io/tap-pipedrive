from tap_pipedrive.stream import PipedriveStream


class StagesStream(PipedriveStream):
    endpoint = 'stages'
    key_properties = ['id', ]
