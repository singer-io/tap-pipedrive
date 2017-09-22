from tap_pipedrive.streams.recents import RecentsStream


class RecentPersonsStream(RecentsStream):
    items = 'person'
    schema = 'persons'
    key_properties = ['id', ]
