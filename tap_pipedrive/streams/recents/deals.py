from tap_pipedrive.streams.recents import RecentsStream


class RecentDealsStream(RecentsStream):
    items = 'deal'
    schema = 'deals'
    key_properties = ['id', ]
    state_field = 'update_time'
