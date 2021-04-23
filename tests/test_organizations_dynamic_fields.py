from tap_tester import connections
from base import PipeDriveBaseTest

class PipedriveDiscovery(PipeDriveBaseTest):

    @staticmethod
    def name():
        return "tap_tester_organizations_dynamic_fields"

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
            if catalog['stream'] == "organizations":
                organization_fields_page_limit = 100

                stream_metadata = catalog['metadata']
                organizations_dynamic_fields = [m for m in stream_metadata if m['breadcrumb'][1] not in self.organizations_static_fields()]

                #Verify count of dynamic fields is more than page limit for organization fields(Pagination)
                self.assertGreater(len(organizations_dynamic_fields), organization_fields_page_limit)
