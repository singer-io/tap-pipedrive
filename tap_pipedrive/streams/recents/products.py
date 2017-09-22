from tap_pipedrive.streams.recents import RecentsStream


class RecentProductsStream(RecentsStream):
    items = 'product'
    schema = 'products'
    key_properties = ['id', ]
