from tap_pipedrive.streams.recents import RecentsStream


class RecentActivitiesStream(RecentsStream):
    items = 'activity'
    schema = 'activities'
    key_properties = ['id', ]
    state_field = 'update_time'
