from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream


class RecentActivitiesStream(DynamicTypingRecentsStream):
    items = 'activity'
    schema = 'activities'
    key_properties = ['id', ]
    state_field = 'update_time'
