from tap_pipedrive.singer.stream import Stream


class CurrenciesStream(Stream):
    endpoint = 'currencies'
    schema = {
        "type": "object",
        "properties": {
            "code": {
                "type": ["null", "string"]
            },
            "name": {
                "type": ["null", "string"]
            },
            "decimal_points": {
                "type": ["null", "integer"]
            },
            "symbol": {
                "type": ["null", "string"]
            },
            "active_flag": {
                "type": ["null", "boolean"]
            },
            "is_custom_flag": {
                "type": ["null", "boolean"]
            },
        }
    }
    key_properties = ['id', ]
