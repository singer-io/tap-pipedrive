from unittest import mock
from tap_pipedrive.tap import PipedriveTap
from tap_pipedrive.streams.dealflow import DealStageChangeStream
import unittest
import pendulum
from pendulum.tz.timezone import Timezone

class Mockresponse:
    def __init__(self, status_code, json, headers=None):
        self.status_code = status_code
        self.text = json
        self.headers = headers

    def json(self):
        return self.text

# function to get mocked response
def get_response(date):
    json = {
        "success": True,
        "data": [
            {
                "object": "dealChange",
                "data": {
                    "id": 1,
                    "field_key": "add_time",
                    "log_time": "2022-04-25 00:00:00",
                }
            },
            {
                "object": "dealChange",
                "data": {
                    "id": 2,
                    "field_key": "add_time",
                    "log_time": date,
                }
            }
        ]
    }
    return Mockresponse(200, json, {"X-RateLimit-Remaining": 10})

class Stream:
    def __init__(self) -> None:
        self.schema = {"dealflow": "dealflow"}
        self.metadata = {}

class Catalog:
    def __init__(self, stream_name):
        self.stream_name = stream_name

    def get_stream(self, stream_name):
        return Stream()

def mock_transform(*args, **kwargs):
    return args[0] # return 1 arg, that is record itself

@mock.patch("singer.Transformer.transform", side_effect = mock_transform)
@mock.patch("tap_pipedrive.stream.PipedriveIterStream.get_deal_ids", return_value = [1])
@mock.patch("tap_pipedrive.tap.PipedriveTap.execute_stream_request")
@mock.patch("tap_pipedrive.tap.PipedriveTap.get_selected_streams", return_value = ["dealflow"])
class TestDealflowBookmarking(unittest.TestCase):
    """
        Test cases to verify we set bookmark minimum 3 hours back for "dealflow" stream
    """

    mock_config = {
        "api_token": "test_api_token",
        "start_date": "2017-01-01T00:00:00Z"
    }

    # create dealflow stream object
    dealflow_obj = DealStageChangeStream()
    # handle some variables which are used when calling "do_sync"
    dealflow_obj.these_deals = [1] # set deal ids
    dealflow_obj.more_ids_to_get = False # set we have no more records
    # set sync start date to desired date
    dealflow_obj.stream_start = pendulum.DateTime(2022, 5, 1, 5, tzinfo=Timezone("UTC")) # now = "2022-05-01 05:00:00"

    def test_now_minus_3_hrs_bookmark(self, mocked_get_selected_streams, mocked_execute_stream_request, mocked_get_deal_ids, mocked_transform):
        """
            Test case to verify we set (now - 3 hours) date as bookmark for 'dealflow' stream as the max bookmark is greater than (now - 3 hrs)
            scenario: max bookmark is greater than (now - 3 hrs)
            assertion: state file contains bookmark of (now - 3 hrs)
        """
        mocked_execute_stream_request.return_value = get_response("2022-05-01 04:00:00") # date with max bookmark value

        # set "PipedriveTap" stream as "DealStageChangeStream"
        tap = PipedriveTap(self.mock_config, {})
        tap.streams = [self.dealflow_obj]

        tap.do_sync(Catalog("dealflow"))
        self.assertEqual(tap.state.get("bookmarks").get("dealflow").get("log_time"), "2022-05-01T02:00:00+00:00")

    def test_max_replication_value_bookmark(self, mocked_get_selected_streams, mocked_execute_stream_request, mocked_get_deal_ids, mocked_transform):
        """
            Test case to verify we set max replication date as bookmark for 'dealflow' stream as the max bookmark is lesser than (now - 3 hrs)
            scenario: max bookmark is lesser than (now - 3 hrs)
            assertion: state file contains bookmark of max bookmark
        """
        mocked_execute_stream_request.return_value = get_response("2022-04-25 00:00:00") # date with max bookmark value

        # set "PipedriveTap" stream as "DealStageChangeStream"
        tap = PipedriveTap(self.mock_config, {})
        tap.streams = [self.dealflow_obj]

        tap.do_sync(Catalog("dealflow"))
        self.assertEqual(tap.state.get("bookmarks").get("dealflow").get("log_time"), "2022-04-25T00:00:00+00:00")
