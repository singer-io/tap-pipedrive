from tap_pipedrive.stream import PipedriveIncrementalStreamUsingSort, PipedriveV1IncrementalStream, DynamicSchemaStream

class NotesStream(DynamicSchemaStream, PipedriveIncrementalStreamUsingSort, PipedriveV1IncrementalStream):
    endpoint = 'notes'
    fields_endpoint = 'noteFields'
    schema = 'notes'
    key_properties = ['id']
    state_field = 'update_time'
    replication_method = 'INCREMENTAL'

    def update_request_params(self, params):
        """
        Update the request parameters for the stream
        """
        params['sort'] = "update_time desc"
        return params
