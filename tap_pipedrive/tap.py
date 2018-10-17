import json
import time
import pendulum
import requests
import singer
import sys

from requests.exceptions import ConnectionError, RequestException
from json import JSONDecodeError
from singer import set_currently_syncing, metadata
from .config import BASE_URL, CONFIG_DEFAULTS
from .exceptions import InvalidResponseException
from .streams import (CurrenciesStream, ActivityTypesStream, FiltersStream, StagesStream, PipelinesStream,
                      GoalsStream, RecentNotesStream, RecentUsersStream, RecentActivitiesStream, RecentDealsStream,
                      RecentFilesStream, RecentOrganizationsStream, RecentPersonsStream, RecentProductsStream,
                      RecentDeleteLogsStream, DealStageChangeStream)


logger = singer.get_logger()


class PipedriveTap(object):
    streams = [
        CurrenciesStream(),
        ActivityTypesStream(),
        StagesStream(),
        FiltersStream(),
        PipelinesStream(),
        GoalsStream(),
        RecentNotesStream(),
        RecentUsersStream(),
        RecentActivitiesStream(),
        RecentDealsStream(),
        RecentFilesStream(),
        RecentOrganizationsStream(),
        RecentPersonsStream(),
        RecentProductsStream(),
        RecentDeleteLogsStream(),
        DealStageChangeStream()
    ]

    def __init__(self, config, state, catalog):
        self.config = self.get_default_config()
        self.config.update(config)
        self.config['start_date'] = pendulum.parse(self.config['start_date'])
        self.state = state
        self.catalog = catalog


    def _load_metadata(self, stream):
        mdata = metadata.new()
        mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)

        for field_name in stream.load_schema()['properties'].keys():
            if field_name in stream.key_properties:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')
        return metadata.to_list(mdata)


    def _discover_streams(self):
        discovered_streams = []

        for stream in self.streams:
            discovered_streams.append({'stream': stream.get_name(), 'tap_stream_id': stream.get_name(), 'schema': stream.load_schema(), 'metadata': self._load_metadata(stream)})
        return discovered_streams


    def do_discover(self):
        logger.info("Starting discover")
        catalog = {"streams": self._discover_streams()}
        json.dump(catalog, sys.stdout, indent=2)
        logger.info("Finished discover")


    def stream_is_selected(self, mdata):
        return mdata.get((), {}).get('selected', False)

    def get_selected_streams(self):
        selected_stream_names = []
        for stream in self.catalog.streams:
            mdata = metadata.to_map(stream.metadata)
            if self.stream_is_selected(mdata):
                selected_stream_names.append(stream.tap_stream_id)
        return selected_stream_names

    def do_sync(self):
        logger.debug('Starting sync')

        selected_stream_names = self.get_selected_streams()

        # resuming when currently_syncing within state
        resume_from_stream = False
        if self.state and 'currently_syncing' in self.state:
            resume_from_stream = self.state['currently_syncing']

        for stream in self.streams:
            stream.tap = self
            stream_name = stream.get_name()
            catalog_entry = self.catalog.get_stream(stream_name)
            mdata = metadata.to_map(catalog_entry.metadata)

            if resume_from_stream:
                if stream.schema == resume_from_stream:
                    logger.info('Resuming from {}'.format(resume_from_stream))
                    resume_from_stream = False
                else:
                    logger.info('Skipping stream {} as resuming from {}'.format(stream.schema, resume_from_stream))
                    continue

            if stream_name not in selected_stream_names:
                logger.info("%s: Skipping - not selected", stream_name)
                continue

            # stream state, from state/bookmark or start_date
            stream.set_initial_state(self.state, self.config['start_date'])

            # currently syncing
            if stream.state_field:
                set_currently_syncing(self.state, stream.schema)
                self.state = singer.write_bookmark(self.state, stream.schema, stream.state_field, str(stream.initial_state))
                singer.write_state(self.state)

            # schema
            stream.write_schema()

            if stream.id_list: # see if we want to iterate over a list of deal_ids

                for deal_id in stream.get_deal_ids(self):
                    is_last_id = False
                    if deal_id == stream.these_deals[-1]: #find out if this is last deal_id in the current set
                        is_last_id = True

                    stream.update_endpoint(deal_id)
                    stream.start = 0   # set back to zero for each new deal_id
                    self.do_paginate(stream, mdata)

                    if not is_last_id:
                        stream.more_items_in_collection = True   #set back to True for pagination of next deal_id request
                    elif is_last_id and stream.more_ids_to_get:  # need to get the next batch of deal_ids
                        stream.more_items_in_collection = True
                        stream.start = stream.next_start
                    else:
                        stream.more_items_in_collection = False

                stream.earliest_state = stream.stream_start
            else:
                # paginate
                self.do_paginate(stream, mdata)

            # update state / bookmarking only when supported by stream
            if stream.state_field:
                self.state = singer.write_bookmark(self.state, stream.schema, stream.state_field,
                                                   str(stream.earliest_state))
            singer.write_state(self.state)

        # clear currently_syncing
        try:
            del self.state['currently_syncing']
        except KeyError as e:
            pass
        singer.write_state(self.state)


    def do_paginate(self, stream, mdata):
        while stream.has_data():

            with singer.metrics.http_request_timer(stream.schema) as timer:
                try:
                    response = self.execute_stream_request(stream)
                except (ConnectionError, RequestException) as e:
                    raise e
                timer.tags[singer.metrics.Tag.http_status_code] = response.status_code

            self.validate_response(response)
            self.rate_throttling(response)
            stream.paginate(response)

            # records with metrics
            with singer.metrics.record_counter(stream.schema) as counter:
                with singer.Transformer(singer.NO_INTEGER_DATETIME_PARSING) as optimus_prime:
                    for row in self.iterate_response(response):
                        row = stream.process_row(row)

                        if not row: # in case of a non-empty response with an empty element
                            continue
                        row = optimus_prime.transform(row, stream.get_schema(), mdata)
                        if stream.write_record(row):
                            counter.increment()
                        stream.update_state(row)

    def get_default_config(self):
        return CONFIG_DEFAULTS

    def iterate_response(self, response):
        payload = response.json()
        return [] if payload['data'] is None else payload['data']

    def execute_stream_request(self, stream):
        params = {
            'start': stream.start,
            'limit': stream.limit
        }
        params = stream.update_request_params(params)
        return self.execute_request(stream.endpoint, params=params)

    def execute_request(self, endpoint, params=None):
        headers = {
            'User-Agent': self.config['user-agent']
        }
        _params = {
            'api_token': self.config['api_token'],
        }
        if params:
            _params.update(params)

        url = "{}/{}".format(BASE_URL, endpoint)
        logger.debug('Firing request at {} with params: {}'.format(url, _params))

        return requests.get(url, headers=headers, params=_params)

    def validate_response(self, response):
        if isinstance(response, requests.Response) and response.status_code == 200:
            try:
                payload = response.json()
                if payload['success'] and 'data' in payload:
                    return True
            except (AttributeError, JSONDecodeError) as e:
                pass

        raise InvalidResponseException("Response with status code {} from Pipedrive API is not valid, "
                                       "wonder why ..".format(response.status_code))

    def rate_throttling(self, response):
        if all(x in response.headers for x in ['X-RateLimit-Remaining', 'X-RateLimit-Reset']):
            if int(response.headers['X-RateLimit-Remaining']) < 1:
                seconds_to_sleep = int(response.headers['X-RateLimit-Reset'])
                logger.debug('Hit API rate limits, no remaining requests per 10 seconds, will sleep '
                             'for {} seconds now.'.format(seconds_to_sleep))
                time.sleep(seconds_to_sleep)
        else:
            logger.debug('Required headers for rate throttling are not present in response header, '
                         'unable to throttle ..')
