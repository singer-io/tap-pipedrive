import os
import singer
import pendulum


logger = singer.get_logger()


class Stream(object):
    endpoint = ''
    key_properties = []
    state_field = None
    state = None

    def get_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                   'schemas/{}.json'.format(self.endpoint))
        schema = singer.utils.load_json(schema_path)
        return schema

    def has_data(self):
        raise NotImplementedError("Implement this method")

    def paginate(self, response):
        raise NotImplementedError("Implement this method")

    def write_schema(self):
        singer.write_schema(self.endpoint, self.get_schema(), key_properties=self.key_properties)

    def write_record(self, row):
        singer.write_record(self.endpoint, row)

    def get_name(self):
        return self.endpoint

    def metrics_http_request_timer(self, response):
        pass

    def update_state(self, row):
        if self.state_field:
            if self.state_field not in row:
                raise Exception('Invalid state field "{}" was not found within row {}'.format(self.state_field, row))

            # nullable update_time breaks bookmarking
            if row[self.state_field] is not None:
                current_state = pendulum.parse(row[self.state_field])

                if self.state_is_newer_or_equal(current_state):
                    self.state = current_state

    def state_is_newer_or_equal(self, current_state):
        raise NotImplementedError("Implement this method")

    def set_initial_state(self, state, start_date):
        try:
            dt = state['bookmarks'][self.get_name()][self.state_field]
            if dt is not None:
                self.state = pendulum.parse(dt)
                return

        except (TypeError, KeyError) as e:
            pass

        self.state = start_date
