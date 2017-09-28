from tap_pipedrive.streams.recents import RecentsStream


class RecentOrganizationsStream(RecentsStream):
    items = 'organization'
    schema = 'organizations'
    key_properties = ['id', ]
    state_field = 'update_time'
