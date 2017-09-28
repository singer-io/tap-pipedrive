from tap_pipedrive.streams.recents import RecentsStream


class RecentDeleteLogsStream(RecentsStream):
    items = 'delete_log'
    schema = 'deleteLogs'
    key_properties = ['id', ]
