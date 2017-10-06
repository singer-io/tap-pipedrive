from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream


class RecentProductsStream(DynamicTypingRecentsStream):
    items = 'product'
    schema = 'products'
    key_properties = ['id', ]
    state_field = 'update_time'
