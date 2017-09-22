import singer
from tap_pipedrive.singer.stream import Stream


logger = singer.get_logger()


class PipedriveStream(Stream):
    start = 0
    limit = 100
    next_start = 100
    more_items_in_collection = True

    def has_data(self):
        return self.more_items_in_collection

    def paginate(self, response):
        payload = response.json()

        if 'additional_data' in payload and 'pagination' in payload['additional_data']:
            logger.info('Paginate: valid response')
            pagination = payload['additional_data']['pagination']
            if 'more_items_in_collection' in pagination:
                self.more_items_in_collection = pagination['more_items_in_collection']

                if 'next_start' in pagination:
                    self.start = pagination['next_start']

        else:
            self.more_items_in_collection = False

        if self.more_items_in_collection:
            logger.info('Stream {} has more data starting at {}'.format(self.endpoint, self.start))
        else:
            logger.info('Stream {} has no more data'.format(self.endpoint))

    def update_request_params(self, params):
        """
        Non recent stream doesn't modify request params
        """
        return params

    def metrics_http_request_timer(self, response):
        with singer.metrics.http_request_timer(self.get_name()) as timer:
            timer.tags[singer.metrics.Tag.http_status_code] = response.status_code
