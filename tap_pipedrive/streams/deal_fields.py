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
        if 'parent_id' in row:
            return None

        # It is expected that the fields which haven't been updated would have
        # replication_key value as None
        # So use add_time as replication_key value
        if row[self.state_field] is None:
            row[self.state_field] = row["add_time"]

        return row
