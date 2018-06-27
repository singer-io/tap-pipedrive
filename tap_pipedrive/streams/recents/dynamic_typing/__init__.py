import singer
from requests import RequestException
from tap_pipedrive.streams.recents import RecentsStream
import re

logger = singer.get_logger()


class DynamicTypingRecentsStream(RecentsStream):
    schema_path = 'schemas/recents/dynamic_typing/{}.json'
    static_fields = []
    fields_endpoint = ''

    # This regex is used to transform column names for custom columns with GUIDs.
    GUID_REGEX = re.compile(r"^[0-9A-Fa-f]{40}")
    SEPARATORS_TRANSLATION = re.compile(r"[-\s]")
    COLUMN_NAME_TRANSLATION = re.compile(r"[^a-zA-Z0-9_]")
    UNDERSCORE_CONSOLIDATION = re.compile(r"_+")

    def sanitize_field_name(self, name):
        result = name.lower()
        result = self.SEPARATORS_TRANSLATION.sub('_', result) # Replace separator characters with underscores
        result = self.COLUMN_NAME_TRANSLATION.sub('', result) # Remove all other non-alphanumeric characters
        return self.UNDERSCORE_CONSOLIDATION.sub('_', result) # Consolidate consecutive underscores

    def get_property_name(self, property):
        if self.GUID_REGEX.match(property.get('key')):
            result = self.sanitize_field_name(property.get('name'))
            logger.info("Translated GUID property key %s to custom field name %s", property.get('key'), result)
            return  result
        else:
            return property.get('key')

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
                        logger.debug("%s, %s, %s, %s", property.get('key'), property.get('name'), property.get('field_type'), property.get('mandatory_flag'))

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

                        property_name = self.get_property_name(property)

                        # allow all dynamic properties to be null since this 
                        # happens in practice probably because a property could
                        # be marked mandatory for some amount of time and not
                        # mandatory for another amount of time
                        property_content['type'].append('null')

                        schema['properties'][property_name] = property_content

            except Exception as e:
                raise e

            self.schema_cache = schema
        return self.schema_cache
