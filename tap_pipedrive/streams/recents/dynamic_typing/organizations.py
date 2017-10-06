from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream


class RecentOrganizationsStream(DynamicTypingRecentsStream):
    items = 'organization'
    schema = 'organizations'
    key_properties = ['id', ]
    state_field = 'update_time'
