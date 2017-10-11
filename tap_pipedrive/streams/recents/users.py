from tap_pipedrive.streams.recents import RecentsStream


class RecentUsersStream(RecentsStream):
    items = 'user'
    schema = 'users'
    key_properties = ['id', ]
    # temporary disabled due current Pipedrive API limitations
    # state_field = 'modified'

    def process_row(self, row):
        return row['data'][0]
