import time
import requests
import singer
from .singer.tap import Tap
from .streams import (CurrenciesStream, NotesStream, ActivityTypesStream, FiltersStream, StagesStream,
                      PipelinesStream, GoalsStream, RecentNotesStream, RecentUsersStream, RecentStagesStream,
                      RecentActivitiesStream, RecentDealsStream, RecentFilesStream, RecentOrganizationsStream,
                      RecentPersonsStream, RecentProductsStream)
from .config import BASE_URL, CONFIG_DEFAULTS
from .exceptions import InvalidResponseException


logger = singer.get_logger()


class PipedriveTap(Tap):
    streams = [
        CurrenciesStream(),
        NotesStream(),
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
        RecentProductsStream()
        # RecentStagesStream(),
    ]

    def get_default_config(self):
        return CONFIG_DEFAULTS

    def iterate_response(self, response):
        payload = response.json()
        return [] if payload['data'] is None else payload['data']

    def execute_request(self, stream):
        headers = {
            'User-Agent': self.config['user-agent']
        }
        params = {
            'api_token': self.config['api_token'],
            'start': stream.start,
            'limit': stream.limit
        }
        url = "{}/{}".format(BASE_URL, stream.endpoint)
        logger.debug('Firing request at {} with start {} and limit {}'.format(url,
                                                                             stream.start,
                                                                             stream.limit))
        params = stream.update_request_params(params)
        logger.debug('Params: {}'.format(params))

        return requests.get(url, headers=headers, params=params)

    def validate_response(self, response):
        if isinstance(response, requests.Response) and response.status_code == 200:
            try:
                payload = response.json()
                if payload['success'] and 'data' in payload:
                    return True

            # TODO narrow down
            except Exception as e:
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
            logger.debug('Required headers for rate throttling are not present in response header, unable to throttle ..')

