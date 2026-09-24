from tap_pipedrive.stream import PipedriveStream


class OrganizationFieldsStream(PipedriveStream):
    endpoint = 'organizationFields'
    schema = 'organization_fields'
    state_field = None
    key_properties = ['field_code']
    replication_method = 'FULL_TABLE'

    def update_request_params(self, params):
        params = {
            'limit': self.limit,
        }
        if self.cursor:
            params['cursor'] = self.cursor
        return params

    def get_name(self):
        return self.schema
