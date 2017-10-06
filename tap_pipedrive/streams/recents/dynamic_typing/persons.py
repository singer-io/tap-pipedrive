from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream


class RecentPersonsStream(DynamicTypingRecentsStream):
    items = 'person'
    schema = 'persons'
    key_properties = ['id', ]
    state_field = 'update_time'
