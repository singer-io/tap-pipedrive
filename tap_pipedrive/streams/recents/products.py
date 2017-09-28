from tap_pipedrive.streams.recents import RecentsStream


class RecentProductsStream(RecentsStream):
    items = 'product'
    schema = 'products'
    key_properties = ['id', ]
    state_field = 'update_time'
