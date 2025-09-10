from tap_pipedrive.stream import DynamicSchemaStream

class DealsStream(DynamicSchemaStream):
    endpoint = 'deals'
    fields_endpoint = 'dealFields'
    schema = 'deals'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None

    def update_request_params(self, params):
        """
        Update the request parameters for the stream
        """
        params = super().update_request_params(params)
        params['status'] = 'open,won,lost,deleted'

        return params
