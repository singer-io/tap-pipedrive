from tap_pipedrive.streams.recents import RecentsStream


class RecentUsersStream(RecentsStream):
    items = 'user'
    schema = 'users'
    key_properties = ['id', ]
    # disabled bookmarking till settled it's data / array
    # state_field = 'modified'
