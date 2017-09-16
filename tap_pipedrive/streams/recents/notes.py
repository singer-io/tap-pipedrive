from tap_pipedrive.streams.recents import RecentsStream


class RecentNotesStream(RecentsStream):
    items = 'note'
    schema = 'notes'
    key_properties = ['id', ]
