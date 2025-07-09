from tap_pipedrive.stream import PipedriveV1IncrementalStream

class CurrenciesStream(PipedriveV1IncrementalStream):
    endpoint = 'currencies'
    schema = 'currency'
    key_properties = ['id']
