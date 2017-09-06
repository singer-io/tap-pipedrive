from tap_pipedrive.singer.stream import Stream


class CurrenciesStream(Stream):
    endpoint = 'currencies'
    key_properties = ['id', ]
