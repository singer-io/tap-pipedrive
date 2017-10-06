from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream


class RecentDealsStream(DynamicTypingRecentsStream):
    items = 'deal'
    schema = 'deals'
    key_properties = ['id', ]
    state_field = 'update_time'
