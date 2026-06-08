from tap_pipedrive.stream import DynamicSchemaStream

class DealsStream(DynamicSchemaStream):
    endpoint = 'deals'
    fields_endpoint = 'dealFields'
    schema = 'deals'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
    additional_fields = ['next_activity_id', 'last_activity_id', 'first_won_time', 'products_count',
                         'files_count', 'notes_count', 'followers_count', 'email_messages_count',
                         'activities_count', 'done_activities_count', 'undone_activities_count', 'participants_count',
                         'last_incoming_mail_time', 'last_outgoing_mail_time']

    def update_request_params(self, params):
        """
        Update the request parameters for the stream
        """
        params = super().update_request_params(params)
        params['status'] = 'open,won,lost,deleted'

        return params
