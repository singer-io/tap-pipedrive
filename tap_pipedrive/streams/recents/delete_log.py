from tap_pipedrive.streams.recents import RecentsStream


class RecentDeleteLogsStream(RecentsStream):
    items = 'delete_log'
    schema = 'delete_log'
    key_properties = ['id', ]
