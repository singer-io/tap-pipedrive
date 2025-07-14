from tap_pipedrive.stream import PipedriveIncrementalStreamUsingSort

class StagesStream(PipedriveIncrementalStreamUsingSort):
    endpoint = 'stages'
    schema = 'stages'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
