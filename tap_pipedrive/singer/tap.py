import pendulum
import singer


logger = singer.get_logger()


class Tap(object):
    streams = []

    def __init__(self, config, state):
        self.config = self.get_default_config()
        self.config.update(config)
        self.config['start_date'] = pendulum.parse(self.config['start_date'])
        self.state = state

    def do_sync(self):
        logger.info('Starting sync')

        for stream in self.streams:
            logger.info('Starting to process stream: {}'.format(stream.get_name()))

            # stream state, from state/bookmark or start_date
            stream.set_initial_state(self.state, self.config['start_date'])

            # schema
            stream.write_schema()

            # paginate
            while stream.has_data():

                response = self.execute_request(stream)
                stream.metrics_http_request_timer(response)
                self.rate_throttling(response)
                stream.paginate(response)
                self.validate_response(response)

                # records with metrics
                with singer.metrics.record_counter(stream.get_name()) as counter:
                    with singer.Transformer(singer.NO_INTEGER_DATETIME_PARSING) as optimus_prime:
                        for row in self.iterate_response(response):
                            row = optimus_prime.transform(row, stream.get_schema())
                            if stream.write_record(row):
                                counter.increment()
                            stream.update_state(row)

            # update state / bookmarking only when supported by stream
            if stream.state_field:
                self.state = singer.write_bookmark(self.state, stream.get_name(), stream.state_field, str(stream.state))
            singer.write_state(self.state)

    def iterate_response(self, response):
        raise NotImplementedError("Implement this method")

    def execute_request(self, stream):
        raise NotImplementedError("Implement this method")

    def validate_response(self, response):
        raise NotImplementedError("Implement this method")

    def get_default_config(self):
        return {}

    def rate_throttling(self, response):
        """
        Rate throttling intended to implement on API level
        """
        return
