import singer
from datetime import datetime

from tap_pipedrive.stream import PipedriveStream

logger = singer.get_logger()


class DealInstallmentsStream(PipedriveStream):
    """
    GET /api/v2/deals/installments returns the installments attached to a
    batch of up to 100 deal ids per request (cursor paginated), rather than
    one deal per request.

    This feature is only available on Pipedrive Growth plan and above.
    Accounts without access receive a 403 from Pipedrive, which is handled
    the same way as any other inaccessible stream: PipedriveStream.check_access
    excludes it from the catalog during discovery instead of failing the tap.
    """
    base_endpoint = 'deals'
    endpoint = 'deals/installments'
    schema = 'deal_installments'
    state_field = None
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    id_list = True

    current_deal_ids = None
    child_ids = []
    more_ids_to_get = False

    def get_name(self):
        return self.schema

    def update_endpoint(self, deal_ids):
        """
        Point the stream at a fresh batch of deal ids and reset the v2
        cursor so each batch's installments are paginated from the
        beginning.
        """
        self.current_deal_ids = deal_ids
        self.cursor = None

    def update_request_params(self, params):
        """
        Installments are a full-table collection scoped to a batch of deal
        ids - only limit/cursor/deal_ids apply.
        """
        params = {'limit': self.limit}
        if self.cursor:
            params['cursor'] = self.cursor
        if self.current_deal_ids:
            params['deal_ids'] = ','.join(str(deal_id) for deal_id in self.current_deal_ids)
        return params

    def get_child_ids(self, tap):
        """
        Paginate GET /api/v2/deals using a local v2 cursor (kept separate
        from self.cursor, which tracks installments pagination) and yield
        each page of deal ids (up to 100, matching the installments
        endpoint's max batch size) as one installments request batch.
        """
        self.stream_start = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        parent_cursor = None
        parent_has_more = True

        while parent_has_more:
            params = {'limit': self.limit}
            if parent_cursor:
                params['cursor'] = parent_cursor

            with singer.metrics.http_request_timer(self.schema) as timer:
                response = tap.execute_request(
                    self.base_endpoint,
                    api_version='api/v2',
                    params=params
                )
                timer.tags[singer.metrics.Tag.http_status_code] = response.status_code

            tap.validate_response(response)
            tap.rate_throttling(response)

            payload = response.json()
            parent_cursor = payload.get('additional_data', {}).get('next_cursor')
            parent_has_more = bool(parent_cursor)
            self.more_ids_to_get = parent_has_more

            page_ids = [deal.get('id') for deal in (payload.get('data') or []) if deal.get('id') is not None]

            if not page_ids:
                continue

            self.child_ids = [page_ids]
            yield page_ids
