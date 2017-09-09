from tap_pipedrive.stream import PipedriveStream


class NotesStream(PipedriveStream):
    endpoint = 'notes'
    key_properties = ['id', ]
