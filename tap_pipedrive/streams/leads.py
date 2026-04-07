from tap_pipedrive.stream import DynamicSchemaStream


class LeadsStream(DynamicSchemaStream):
    endpoint = 'leads'
    fields_endpoint = 'dealFields'
    schema = 'leads'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    api_version = 'v1'
    cursor = None

    INCLUDE_FIELDS = (
        'last_activity_id',
        'last_incoming_mail_time',
        'last_outgoing_mail_time',
        'email_messages_count',
        'activities_count',
        'done_activities_count',
        'undone_activities_count',
        'participants_count',
        'files_count',
        'notes_count',
        'followers_count',
    )

    def paginate(self, response):
        payload = response.json()
        if payload.get('additional_data') and 'pagination' in payload['additional_data']:
            pagination = payload['additional_data']['pagination']
            if 'more_items_in_collection' in pagination:
                self.more_items_in_collection = pagination['more_items_in_collection']
                if 'next_start' in pagination:
                    self.start = pagination['next_start']
        else:
            self.more_items_in_collection = False

    def update_request_params(self, params):
        params = {
            'limit': self.limit,
            'start': self.start,
            'sort': 'update_time ASC',
        }
        params['include_fields'] = ','.join(self.INCLUDE_FIELDS)
        return params

    def process_row(self, row):
        value = row.get('value')
        if value and isinstance(value, dict):
            row['lead_value_amount'] = value.get('amount')
            row['lead_value_currency'] = value.get('currency')
        else:
            row['lead_value_amount'] = None
            row['lead_value_currency'] = None
        return row
