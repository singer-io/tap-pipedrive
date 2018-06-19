import time
import pendulum
import requests
import singer
from requests.exceptions import ConnectionError, RequestException
from json import JSONDecodeError
from singer import set_currently_syncing
from .config import BASE_URL, CONFIG_DEFAULTS
from .exceptions import InvalidResponseException
from .streams import (CurrenciesStream, ActivityTypesStream, FiltersStream, StagesStream, PipelinesStream,
                      GoalsStream, RecentNotesStream, RecentUsersStream, RecentActivitiesStream, RecentDealsStream,
                      RecentFilesStream, RecentOrganizationsStream, RecentPersonsStream, RecentProductsStream,
                      RecentDeleteLogsStream)


logger = singer.get_logger()


class PipedriveTap(object):
    streams = [
        CurrenciesStream(),
        ActivityTypesStream(),
        FiltersStream(),
        StagesStream(),
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
        RecentDeleteLogsStream()
    ]

    def __init__(self, config, state):
        self.config = self.get_default_config()
        self.config.update(config)
        self.config['start_date'] = pendulum.parse(self.config['start_date'])
        self.state = state

    def do_sync(self):
        logger.debug('Starting sync')

        # resuming when currently_syncing within state
        resume_from_stream = False
        if self.state and 'currently_syncing' in self.state:
            resume_from_stream = self.state['currently_syncing']

        for stream in self.streams:
            stream.tap = self

            if resume_from_stream:
                if stream.schema == resume_from_stream:
                    logger.info('Resuming from {}'.format(resume_from_stream))
                    resume_from_stream = False
                else:
                    logger.info('Skipping stream {} as resuming from {}'.format(stream.schema, resume_from_stream))
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

            # paginate
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
                            row = optimus_prime.transform(row, stream.get_schema())
                            if stream.write_record(row):
                                counter.increment()
                            stream.update_state(row)

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

