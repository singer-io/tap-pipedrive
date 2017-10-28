import singer
from requests import RequestException
from tap_pipedrive.streams.recents import RecentsStream


logger = singer.get_logger()


class DynamicTypingRecentsStream(RecentsStream):
    schema_path = 'schemas/recents/dynamic_typing/{}.json'
    static_fields = []
    fields_endpoint = ''

    def get_schema(self):
        if not self.schema_cache:
            schema = self.load_schema()

            try:
                fields_response = self.tap.execute_request(endpoint=self.fields_endpoint)
            except (ConnectionError, RequestException) as e:
                raise e

            try:
                assert fields_response.status_code == 200, 'Invalid response from API, ' \
                                                           'status code is {}'.format(fields_response.status_code)
                payload = fields_response.json()

                for property in payload['data']:
                    if property['key'] not in self.static_fields:
                        logger.debug(property['key'], property['field_type'], property['mandatory_flag'])

                        if property['key'] in schema['properties']:
                            logger.warn('Dynamic property "{}" overrides with type {} existing entry in ' \
                                        'static JSON schema of {} stream.'.format(
                                            property['key'],
                                            property['field_type'],
                                            self.schema
                                        )
                            )

                        property_content = {
                            'type': []
                        }

                        if property['field_type'] in ['int']:
                            property_content['type'].append('integer')

                        elif property['field_type'] in ['timestamp']:
                            property_content['type'].append('string')
                            property_content['format'] = 'date-time'

                        else:
                            property_content['type'].append('string')

                        # allow all dynamic properties to be null since this 
                        # happens in practice probably because a property could
                        # be marked mandatory for some amount of time and not
                        # mandatory for another amount of time
                        property_content['type'].append('null')

                        schema['properties'][property['key']] = property_content

            except Exception as e:
                raise e

            self.schema_cache = schema
        return self.schema_cache
