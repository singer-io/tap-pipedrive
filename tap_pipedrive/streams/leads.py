from tap_pipedrive.stream import DynamicSchemaStream


class LeadsStream(DynamicSchemaStream):
    endpoint = 'leads'
    fields_endpoint = 'dealFields'
    schema = 'leads'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    state_field = None
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

    # Track which endpoints we still need to paginate through
    _endpoints = ['leads', 'leads/archived']
    _endpoint_index = 0

    def paginate(self, response):
        payload = response.json()
        if payload.get('additional_data') and 'pagination' in payload['additional_data']:
            pagination = payload['additional_data']['pagination']
            if 'more_items_in_collection' in pagination:
                self.more_items_in_collection = pagination['more_items_in_collection']
                if 'next_start' in pagination:
                    self.start = pagination['next_start']

                # Current endpoint exhausted — move to the next one if there is one
                if not self.more_items_in_collection:
                    self._endpoint_index += 1
                    if self._endpoint_index < len(self._endpoints):
                        self.endpoint = self._endpoints[self._endpoint_index]
                        self.start = 0
                        self.more_items_in_collection = True
        else:
            # No pagination block at all — treat as exhausted and advance
            self._endpoint_index += 1
            if self._endpoint_index < len(self._endpoints):
                self.endpoint = self._endpoints[self._endpoint_index]
                self.start = 0
                self.more_items_in_collection = True
            else:
                self.more_items_in_collection = False

    def update_request_params(self, params):
        params = {
            'limit': self.limit,
            'start': self.start,
            'sort': 'add_time ASC',
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