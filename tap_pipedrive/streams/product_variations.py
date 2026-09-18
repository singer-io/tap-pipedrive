import singer
from datetime import datetime

from tap_pipedrive.stream import PipedriveStream

logger = singer.get_logger()


class ProductVariationsStream(PipedriveStream):
    """
    For every product id returned by GET /api/v2/products, fetches that
    product's variations via GET /api/v2/products/{id}/variations.
    """
    base_endpoint = 'products'
    id_endpoint = 'products/{}/variations'
    schema = 'product_variations'
    state_field = None
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    parent = 'products'
    id_list = True

    current_product_id = None
    child_ids = []
    more_ids_to_get = False

    def get_name(self):
        return self.schema

    def update_endpoint(self, product_id):
        """
        Point the stream at the variations endpoint for `product_id` and
        reset the v2 cursor so each product's variations are paginated
        from the beginning.
        """
        self.current_product_id = product_id
        self.cursor = None
        self.endpoint = self.id_endpoint.format(product_id)

    def update_request_params(self, params):
        """
        Variations are a full-table child collection - only limit/cursor
        apply.
        """
        params = {'limit': self.limit}
        if self.cursor:
            params['cursor'] = self.cursor
        return params

    def get_child_ids(self, tap):
        """
        Paginate GET /api/v2/products using a local v2 cursor (kept
        separate from self.cursor, which tracks the variations/child
        pagination) and yield each product id in turn.
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

            page_ids = [product.get('id') for product in (payload.get('data') or []) if product.get('id') is not None]

            if not page_ids:
                continue

            self.child_ids = page_ids
            for product_id in page_ids:
                yield product_id
