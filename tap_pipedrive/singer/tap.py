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
            logger.info('Starting to process stream: {}'.format(stream.endpoint))

            # schema
            singer.write_schema(stream.endpoint, stream.schema, key_properties=stream.key_properties)

            # records
            response = self.execute_request(stream)
            self.validate_response(response)

            for row in self.iterate_response(response):
                singer.write_record(stream.endpoint, row)

    def iterate_response(self, response):
        raise NotImplementedError("Implement this method")

    def execute_request(self, stream):
        raise NotImplementedError("Implement this method")

    def validate_response(self, response):
        raise NotImplementedError("Implement this method")

    def get_default_config(self):
        return {}
