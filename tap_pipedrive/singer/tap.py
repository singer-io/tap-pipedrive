import singer


logger = singer.get_logger()


class Tap(object):
    streams = []

    def __init__(self, config, state):
        self.config = self.get_default_config()
        self.config.update(config)
        self.state = state

    def do_sync(self):
        logger.info('Starting sync')

        for stream in self.streams:
            logger.info('Starting to process stream: {}'.format(stream.get_name()))

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
                    for row in self.iterate_response(response):
                        stream.write_record(row)
                        counter.increment()

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
