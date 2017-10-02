from tap_pipedrive.stream import PipedriveStream


class CurrenciesStream(PipedriveStream):
    endpoint = 'currencies'
    schema = 'currencies'
    key_properties = ['id', ]
