from tap_pipedrive.stream import PipedriveIncrementalStreamUsingSort

class ProductsStream(PipedriveIncrementalStreamUsingSort):
    endpoint = 'products'
    schema = 'products'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
