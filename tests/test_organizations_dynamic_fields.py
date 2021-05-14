from tap_tester import menagerie, connections
from base import PipedriveBaseTest

class PipedriveDiscovery(PipedriveBaseTest):

    @staticmethod
    def name():
        return "tap_tester_organizations_dynamic_fields"

    def organizations_static_fields(self):
        return ['active_flag', 'activities_count', 'add_time', 'address', 'address_admin_area_level_1',
                'address_admin_area_level_2', 'address_country', 'address_formatted_address', 'address_locality',
                'address_postal_code', 'address_route', 'address_street_number', 'address_sublocality',
                'address_subpremise', 'category_id', 'cc_email', 'closed_deals_count', 'company_id',
                'country_code', 'done_activities_count', 'email_messages_count', 'files_count', 'first_char',
                'followers_count', 'id', 'last_activity_date', 'last_activity_id', 'lost_deals_count', 'name',
                'next_activity_date', 'next_activity_id', 'next_activity_time', 'notes_count', 'open_deals_count',
                'owner_id', 'owner_name', 'people_count', 'picture_id', 'reference_activities_count',
                'related_closed_deals_count', 'related_lost_deals_count', 'related_open_deals_count',
                'related_won_deals_count', 'timeline_last_activity_time', 'timeline_last_activity_time_by_owner',
                'undone_activities_count', 'update_time', 'visible_to', 'won_deals_count']

    def test_organizations_dynamic_fields(self):
        """
        Run tap in check mode and verify more than one page is retruned for dynamic fields. 
        """
        conn_id = connections.ensure_connection(self)

        # run and verify the tap in discovermode. 
        found_catalog = self.run_and_verify_check_mode(conn_id)

        # Verify number of dynamic fields in organizations stream metadata
        # (Need enough dynamic fields for organizations)
        for catalog in found_catalog:
            if catalog['stream_name'] == "organizations":
                organization_fields_page_limit = 100

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                schema_fields = schema_and_metadata.get('annotated-schema').get('properties').keys()
                organizations_dynamic_fields = [field for field in schema_fields if field not in self.organizations_static_fields()]

                #Verify count of dynamic fields is more than page limit for organization fields(Pagination)
                self.assertGreater(len(organizations_dynamic_fields), organization_fields_page_limit)
