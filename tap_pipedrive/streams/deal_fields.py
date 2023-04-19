from tap_pipedrive.stream import PipedriveStream


class DealFields(PipedriveStream):
    endpoint = 'dealFields'
    schema = 'deal_fields'
    state_field = 'update_time'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'

    def get_name(self):
        return self.schema

    def process_row(self, row):
        # Ignore child fields as they don't have id(PK) and update_time(replication_key)
        return None if 'parent_id' in row else row
