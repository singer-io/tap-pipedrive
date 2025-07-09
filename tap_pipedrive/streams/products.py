from tap_pipedrive.stream import DynamicSchemaStream, PipedriveIncrementalStreamUsingSort

class ProductsStream(DynamicSchemaStream, PipedriveIncrementalStreamUsingSort):
    endpoint = 'products'
    fields_endpoint = 'productFields'
    schema = 'products'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
