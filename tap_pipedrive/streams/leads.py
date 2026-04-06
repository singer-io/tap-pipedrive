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
