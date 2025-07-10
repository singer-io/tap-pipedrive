import os
import singer
import pendulum
from datetime import datetime
from requests.exceptions import RequestException

logger = singer.get_logger()

class PipedriveStream(object):
    tap = None
    endpoint = ''
    fields_endpoint = ''
    key_properties = []
    state_field = None
    initial_state = None
    earliest_state = None
    schema = ''
    schema_path = 'schemas/{}.json'
    schema_cache = None
    replication_method = 'FULL_TABLE'
    api_version = 'api/v2'
    cursor = None

    start = 0
    limit = 100
    next_start = 100
    more_items_in_collection = True

    id_list = False

    def get_schema(self):
        if not self.schema_cache:
            self.schema_cache = self.load_schema()
        return self.schema_cache

    def load_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   self.schema_path.format(self.schema))
        schema = singer.utils.load_json(schema_path)
        return schema

    def write_schema(self):
        singer.write_schema(self.schema, self.get_schema(), key_properties=self.key_properties)

    def get_name(self):
        return self.endpoint

    def update_state(self, row):
        """
        Update the state of the stream
        """
        current_bookmark = row.get(self.state_field)
        current_bookmark = datetime.strptime(current_bookmark, "%Y-%m-%dT%H:%M:%S.000000Z").strftime("%Y-%m-%dT%H:%M:%SZ") if current_bookmark else None
        if current_bookmark and current_bookmark >= self.earliest_state:
            self.earliest_state = current_bookmark

    def set_initial_state(self, state, start_date):
        """
        Set the initial state of the stream
        """
        self.initial_state = state.get("bookmarks", {}).get(self.schema, {}).get(self.state_field) or start_date
        self.earliest_state = self.initial_state

    def has_data(self):
        return self.more_items_in_collection

    def paginate(self, response):
        """
        Implement cursor based pagination
        """
        payload = response.json()
        next_cursor = payload.get("additional_data", {}).get("next_cursor")
        if next_cursor:
            self.cursor = next_cursor
            self.more_items_in_collection = True
            logger.info('Stream {} has more data starting at cursor {}'.format(self.schema, self.cursor))
        else:
            self.more_items_in_collection = False
            logger.info('Stream {} has no more data'.format(self.schema))

    def update_request_params(self, params):
        """
        Update the request parameters for the stream
        """
        params = {
            'limit': self.limit,
            'updated_since': self.initial_state,
            "sort_by": "update_time"
        }
        if self.cursor:
            params['cursor'] = self.cursor

        return params

    def write_record(self, row):
        """
        Write the record to the stream
        """
        current_bookmark = row.get(self.state_field)
        if not current_bookmark or current_bookmark >= self.initial_state:
            return True

        return False

    def process_row(self, row):
        return row

class PipedriveV1IncrementalStream(PipedriveStream):
    api_version = 'v1'

    def update_request_params(self, params):
        """
        Update the request parameters for the stream
        """
        return params

    def paginate(self, response):
        """
        Implement page based pagination
        """
        payload = response.json()
        if payload.get('additional_data') and 'pagination' in payload['additional_data']:
            pagination = payload['additional_data']['pagination']
            if 'more_items_in_collection' in pagination:
                logger.debug('Stream {} has more data starting at {}'.format(self.schema, self.start))
                self.more_items_in_collection = pagination['more_items_in_collection']

                if 'next_start' in pagination:
                    self.start = pagination['next_start']

        else:
            logger.debug('Stream {} has no more data'.format(self.schema))
            self.more_items_in_collection = False

class RecentsStream(PipedriveV1IncrementalStream):
    recent_endpoint = 'recents'
    items = None

    def update_request_params(self, params):
        """
        /GET endpoint does not all to filter by updated_at for some of the endpoints.
        Also, It is not good to fetch all records every time.
        
        /recents endpoint allows to filter by since_timestamp but it returns past 1 month data.
        
        So, use combination of both
        """
        if self.initial_state < pendulum.now().subtract(months=1).strftime("%Y-%m-%dT%H:%M:%SZ"):
            return super().update_request_params(params)
        else:
            params.update({
                'since_timestamp': datetime.strptime(self.initial_state, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M:%S"),
                'items': self.items
            })
            self.endpoint = self.recent_endpoint

            return params

    def process_row(self, row):
        """
        user response is list of dicts while other responses are dicts only
        """
        if self.endpoint == self.recent_endpoint:
            if isinstance(row['data'], dict):
                return row['data']
            else:
                return row['data'][0]
        else:
            return row

class PipedriveIterStream(PipedriveV1IncrementalStream):
    id_list = True
    api_version = 'v1'
    cursor = None
    deal_replication_key = "deal_update_time"

    def get_deal_ids(self, tap):

        # note when the stream starts syncing
        self.stream_start = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") # explicitly set timezone to UTC

        # create checkpoint at inital_state to only find stage changes more recent than initial_state (bookmark)
        checkpoint = self.initial_state
        deal_bookmark = tap.state.get("bookmarks", {}).get(self.schema, {}).get(self.deal_replication_key) or tap.config['start_date']
        params = {
            'limit': self.limit,
            "updated_since": deal_bookmark,
            "sort_by": "update_time"
        }

        while self.more_items_in_collection:
            self.endpoint = self.base_endpoint
            if self.cursor:
                params['cursor'] = self.cursor

            with singer.metrics.http_request_timer(self.schema) as timer:
                try:
                    response = tap.execute_request(self.endpoint, api_version="api/v2", params=params)
                except (ConnectionError, RequestException) as e:
                    raise e
                timer.tags[singer.metrics.Tag.http_status_code] = response.status_code

            tap.validate_response(response)
            tap.rate_throttling(response)
            PipedriveStream.paginate(self, response)

            self.more_ids_to_get = self.more_items_in_collection  # note if there are more pages of ids to get
            self.next_start = self.start  # note pagination for next loop

            if not response.json().get('data'):
                continue

            # find all deals ids for deals added or with stage changes after start and before stop
            this_page_ids = self.find_deal_ids(response.json()['data'], start=checkpoint, stop=self.stream_start)

            self.these_deals = this_page_ids  # need the list of deals to check for last id in the tap
            for deal_id in this_page_ids:
                yield deal_id

            self.state = singer.write_bookmark(tap.state, self.schema, self.deal_replication_key, response.json().get('data')[-1]["update_time"])


    def find_deal_ids(self, data, start, stop):

        # find all deals that were *added* after the start time and before the stop time
        added_ids = [data[i]['id']
                     for i in range(len(data))
                     if (data[i]['add_time'] is not None
                         and start <= data[i]['add_time'] < stop)]

        # find all deals that a) had a stage change at any time (i.e., the stage_change_time is not None),
        #                     b) had a stage change after the start time and before the stop time, and
        #                     c) are not in added_ids
        changed_ids = [data[i]['id']
                       for i in range(len(data))
                       if (data[i]['id'] not in added_ids)
                       and (data[i]['stage_change_time'] is not None
                            and start <= data[i]['stage_change_time'] < stop)]
        return added_ids + changed_ids

class PipedriveIncrementalStreamUsingSort(PipedriveStream):

    sort_by = 'update_time'
    sort_direction = 'desc'

    def update_request_params(self, params):
        """
        Update the request parameters with sorting options
        """
        params = {
            'limit': self.limit,
            "sort_by": "update_time",
            "sort_direction": "desc"
        }
        if self.cursor:
            params['cursor'] = self.cursor

        return params

    def write_record(self, row):
        """
        Write the record to the stream
        """
        current_bookmark = row.get(self.state_field)
        if not current_bookmark or current_bookmark >= self.initial_state:
            return True

        # Stop fetching if the current replication value is less than the earliest state(bookmark)
        self.more_items_in_collection = False
        return False

    def update_state(self, row):
        """
        Update the state only after the stream has been fully processed.
        Because it fetched all records in descending order, the first records will have latest replication value.
        So if some interruption happens, it would miss some records.
        """
        if self.more_items_in_collection:
            return

        current_bookmark = row.get(self.state_field)
        current_bookmark = datetime.strptime(current_bookmark, "%Y-%m-%dT%H:%M:%S.000000Z").strftime("%Y-%m-%dT%H:%M:%SZ") if current_bookmark else None
        if current_bookmark and current_bookmark >= self.earliest_state:
            self.earliest_state = current_bookmark

class DynamicSchemaStream(PipedriveStream):
    static_fields = []
    fields_more_items_in_collection = True

    def get_schema(self):
        if not self.schema_cache:
            schema = self.load_schema()

            while self.fields_more_items_in_collection:
                fields_params = {
                    "limit" : self.limit,
                    "start" : self.start
                } 

                with singer.metrics.http_request_timer(self.schema) as timer:
                    try:
                        fields_response = self.tap.execute_request(endpoint=self.fields_endpoint, api_version= 'v1', params=fields_params)
                    except (ConnectionError, RequestException) as e:
                        raise e

                    timer.tags[singer.metrics.Tag.http_status_code] = fields_response.status_code

                    try: 
                        properties = fields_response.json()
                        for prop in properties['data']:
                            if prop['key'] not in schema['properties']:
                                schema['properties'][prop['key']] = prop
                        
                                property_content = {
                                    'type': ['null']
                                }
                                if prop['field_type'] in ['int']:
                                    property_content['type'].append('integer')
                                elif prop['field_type'] in ['date']:
                                    property_content['type'].append('string')
                                    property_content['format'] = 'date-time'
                                else:
                                    property_content['type'].append('string')

                                schema['properties'][prop['key']] = property_content

                        if properties.get('additional_data') and 'pagination' in properties['additional_data']:
                            pagination = properties['additional_data']['pagination']
                            if 'more_items_in_collection' in pagination:
                                self.fields_more_items_in_collection = pagination['more_items_in_collection']
                                if 'next_start' in pagination:
                                    self.start = pagination['next_start']
                        else:
                            self.fields_more_items_in_collection = False
                    except:
                        pass

            self.schema_cache = schema

        return self.schema_cache
