from tap_pipedrive.stream import PipedriveStream
from datetime import datetime
import singer

LOGGER = singer.get_logger()

class PersonsStream(PipedriveStream):
    endpoint = 'persons'
    schema = 'persons'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
