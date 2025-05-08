from tap_pipedrive.stream import PipedriveStream
import singer

LOGGER = singer.get_logger()

class ActivitiesStream(PipedriveStream):
    endpoint = 'activities'
    schema = 'activities'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    api_version = 'api/v2'
    cursor = None

    def update_request_params(self, params):
        """
        Filter recents enpoint data with items and since_timestamp
        """
        params = {
            'limit': self.limit,
            'updated_since': self.initial_state,
            "sort_by": "update_time"
        }
        if self.cursor:
            params['cursor'] = self.cursor

        return params

    def set_initial_state(self, state, start_date):
        """
        Override set_initial_state to set bookmark as it is for activities stream
        """
        self.initial_state = state.get("bookmarks", {}).get(self.schema, {}).get(self.state_field) or start_date
        self.earliest_state = self.initial_state


    def paginate(self, response):
        """
        Paginate the response to get the next page of data
        """
        payload = response.json()
        next_cursor = payload.get("additional_data", {}).get("next_cursor")
        if next_cursor:
            self.cursor = next_cursor
            self.more_items_in_collection = True
            LOGGER.info('Stream {} has more data starting at cursor {}'.format(self.schema, self.cursor))
        else:
            self.more_items_in_collection = False
            LOGGER.info('Stream {} has no more data'.format(self.schema))

    def write_record(self, row):
        singer.write_record(self.schema, row)
        return True

    def update_state(self, row):
        self.earliest_state = row[self.state_field]