import singer
from tap_pipedrive.stream import PipedriveIterStream

class DealStageChangeStream(PipedriveIterStream):
	base_endpoint = 'deals'
	id_endpoint = 'deals/{}/flow'
	schema = 'dealflow'
	state_field = 'log_time'
	key_properties = ['id', ]

	def get_name(self):
	    return self.schema

	def process_row(self, row):
		if row['object'] == 'dealChange':
			return row['data']

	def update_endpoint(self, deal_id):
		self.endpoint = self.id_endpoint.format(deal_id)