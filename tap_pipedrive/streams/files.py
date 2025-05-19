from tap_pipedrive.stream import PipedriveV1IncrementalStream
import singer

LOGGER = singer.get_logger()

class FilesStream(PipedriveV1IncrementalStream):
    endpoint = 'files'
    schema = 'files'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
