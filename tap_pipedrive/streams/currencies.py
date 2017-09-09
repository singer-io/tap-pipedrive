from tap_pipedrive.stream import PipedriveStream


class CurrenciesStream(PipedriveStream):
    endpoint = 'currencies'
    key_properties = ['id', ]
