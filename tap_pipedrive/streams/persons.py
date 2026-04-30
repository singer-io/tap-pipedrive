from tap_pipedrive.stream import DynamicSchemaStream

class PersonsStream(DynamicSchemaStream):
    endpoint = 'persons'
    fields_endpoint = 'personFields'
    schema = 'persons'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    state_field = 'update_time'
    cursor = None
    additional_fields = ['next_activity_id', 'last_activity_id', 'open_deals_count', 'related_open_deals_count',
                         'closed_deals_count', 'related_closed_deals_count', 'participant_open_deals_count',
                         'participant_closed_deals_count', 'email_messages_count', 'activities_count',
                         'done_activities_count', 'undone_activities_count', 'files_count', 'notes_count',
                         'followers_count', 'won_deals_count', 'related_won_deals_count', 'lost_deals_count',
                         'related_lost_deals_count', 'last_incoming_mail_time', 'last_outgoing_mail_time']