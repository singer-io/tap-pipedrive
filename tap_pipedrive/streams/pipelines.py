from tap_pipedrive.stream import PipedriveIncrementalStreamUsingSort

class PipelinesStream(PipedriveIncrementalStreamUsingSort):
    endpoint = 'pipelines'
    schema = 'pipelines'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
