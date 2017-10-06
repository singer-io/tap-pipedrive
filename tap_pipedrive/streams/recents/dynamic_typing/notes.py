from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream


class RecentNotesStream(DynamicTypingRecentsStream):
    items = 'note'
    schema = 'notes'
    key_properties = ['id', ]
    state_field = 'update_time'
