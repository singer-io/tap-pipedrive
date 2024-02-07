from tap_pipedrive.stream import PipedriveStream
from tap_pipedrive.streams.recents.dynamic_typing import DynamicTypingRecentsStream
from requests import RequestException
import singer
import os

logger = singer.get_logger()

class DealsBetaStream(DynamicTypingRecentsStream):
    schema_path = 'schemas/recents/dynamic_typing/{}.json'
    endpoint = 'deals/collection'
    schema = 'deals_beta'
    key_properties = ['id', ]
    state_field = 'update_time'
    fields_endpoint = 'dealFields'
    static_fields = ['active', 'activities_count', 'add_time', 'cc_email', 'close_time', 'creator_user_id', 'currency',
                     'deleted', 'done_activities_count', 'email_messages_count', 'expected_close_date', 'files_count',
                     'first_won_time', 'followers_count', 'formatted_value', 'formatted_weighted_value', 'id',
                     'last_activity_date', 'last_activity_id', 'last_incoming_mail_time', 'last_outgoing_mail_time',
                     'lost_reason', 'lost_time', 'next_activity_date', 'next_activity_duration', 'next_activity_id',
                     'next_activity_note', 'next_activity_subject', 'next_activity_time', 'next_activity_type',
                     'notes_count', 'org_hidden', 'org_id', 'org_name', 'owner_name', 'participants_count',
                     'person_hidden', 'person_id', 'person_name', 'pipeline_id', 'products_count', 'probability',
                     'reference_activities_count', 'rotten_time', 'stage_change_time', 'stage_id', 'stage_order_nr',
                     'status', 'title', 'undone_activities_count', 'update_time', 'user_id', 'value', 'visible_to',
                     'weighted_value', 'won_time', 'group_id', 'group_name', 'renewal_type']

    def update_request_params(self, params):
        if self.start and self.start != 0:
            params.update({
                "cursor":self.start,
                "since": self.initial_state.subtract(seconds=1).to_datetime_string(),
                })
            params.pop("start", None)
            params.pop("limit", None)
        return params

    def paginate(self, response):
        payload = response.json()

        if 'additional_data' in payload and 'next_cursor' in payload['additional_data']:
            logger.info('Paginate: %s', payload['additional_data'])
            pagination = payload['additional_data']
            cursor = payload['additional_data']["next_cursor"]
            if cursor:
                self.more_items_in_collection = True
                self.start = cursor
            else:
                self.more_items_in_collection = False
        else:
            self.start = None
            self.more_items_in_collection = False

        if self.more_items_in_collection:
            logger.debug('Stream {} has more data starting at {}'.format(self.schema, self.start))
        else:
            logger.debug('Stream {} has no more data'.format(self.schema))

    def process_row(self, row):
        return row