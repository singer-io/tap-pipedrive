from tap_pipedrive.stream import PipedriveV1IncrementalStream


class LeadLabelsStream(PipedriveV1IncrementalStream):
    endpoint = 'leadLabels'
    schema = 'lead_labels'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    state_field = None
