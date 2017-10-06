import singer
from tap_pipedrive.streams.recents import RecentsStream


logger = singer.get_logger()


class DynamicTypingRecentsStream(RecentsStream):
    schema_path = 'schemas/recents/dynamic_typing/{}.json'
