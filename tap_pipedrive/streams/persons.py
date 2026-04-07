from tap_pipedrive.stream import DynamicSchemaStream

class PersonsStream(DynamicSchemaStream):
    endpoint = 'persons'
    fields_endpoint = 'personFields'
    schema = 'persons'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None

    INCLUDE_FIELDS = (
        'last_incoming_mail_time',
        'last_outgoing_mail_time',
    )

    def update_request_params(self, params):
        params = super().update_request_params(params)
        params['include_fields'] = ','.join(self.INCLUDE_FIELDS)
        return params
