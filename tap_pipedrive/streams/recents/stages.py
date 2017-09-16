from tap_pipedrive.streams.recents import RecentsStream


class RecentStagesStream(RecentsStream):
    items = 'stage'
    schema = 'stages'
    key_properties = ['id', ]
