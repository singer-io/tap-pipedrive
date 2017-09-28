import singer
import pendulum
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
            logger.debug('Paginate: valid response')
            pagination = payload['additional_data']['pagination']
            if 'more_items_in_collection' in pagination:
                self.more_items_in_collection = pagination['more_items_in_collection']

                if 'next_start' in pagination:
                    self.start = pagination['next_start']

        else:
            self.more_items_in_collection = False

        if self.more_items_in_collection:
            logger.debug('Stream {} has more data starting at {}'.format(self.endpoint, self.start))
        else:
            logger.debug('Stream {} has no more data'.format(self.endpoint))

    def update_request_params(self, params):
        """
        Non recent stream doesn't modify request params
        """
        return params

    def metrics_http_request_timer(self, response):
        with singer.metrics.http_request_timer(self.get_name()) as timer:
            timer.tags[singer.metrics.Tag.http_status_code] = response.status_code

    def state_is_newer_or_equal(self, current_state):
        if self.state is None:
            self.state = current_state
            return True

        if current_state >= self.state:
            self.state = current_state
            return True

        return False

    def write_record(self, row):
        if self.record_is_newer_equal_null(row):
            singer.write_record(self.endpoint, row)
            return True
        return False

    def record_is_newer_equal_null(self, row):
        # no bookmarking in stream or state is null
        if not self.state_field or self.state is None:
            return True

        # state field is null
        if self.get_row_state(row) is None:
            return True

        # newer or equal
        current_state = pendulum.parse(self.get_row_state(row))
        if current_state >= self.state:
            return True

        return False

    def get_row_state(self, row):
        return row[self.state_field]
