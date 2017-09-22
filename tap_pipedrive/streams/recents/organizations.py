from tap_pipedrive.streams.recents import RecentsStream


class RecentOrganizationsStream(RecentsStream):
    items = 'organization'
    schema = 'organizations'
    key_properties = ['id', ]
