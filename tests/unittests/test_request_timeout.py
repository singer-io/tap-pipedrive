import tap_pipedrive.tap as _tap
import unittest
import requests
from unittest import mock


@mock.patch("requests.get")
class TestRequestTimeoutValue(unittest.TestCase):

    def test_config_without_request_timeout(self, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc"} # No request_timeout passed in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=300) # Expected timeout

    def test_integer_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc", "request_timeout": 100} # integer timeout in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=100.0) # Expected timeout

    def test_float_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc", "request_timeout": 100.5} # float timeout in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=100.5) # Expected timeout

    def test_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc", "request_timeout": '100'} # string format timeout in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=100.0) # Expected timeout

    def test_empty_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc", "request_timeout": ''} # empty string in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=300.0) # Expected timeout

    def test_zero_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc", "request_timeout": 0.0} # zero value in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=300.0) # Expected timeout

    def test_zero_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc", "request_timeout": '0.0'} # zero value in config
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object with config
        pipedrive_tap = _tap.PipedriveTap(config, state)
        # Call refresh_token method which call requests.get with timeout
        pipedrive_tap.execute_request(endpoint, api_version='v1')

        # Verify requests.get is called with expected timeout
        mocked_request.assert_called_with('https://api.pipedrive.com/v1/xyz',
                                          headers={'User-Agent': 'tap-pipedrive (+support@stitchdata.com)',
                                                   'Accept-Encoding': 'application/json'},
                                          params={'api_token': 'abc'},
                                          timeout=300.0) # Expected timeout

@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch("requests.get", side_effect = requests.exceptions.Timeout)
    def test_request_timeout_backoff_execute_request(self, mocked_request, mocked_sleep):
        """
            Verify execute_request function is backoff for 5 times on Timeout exceeption
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc"}
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object
        pipedrive_tap = _tap.PipedriveTap(config, state)
        try:
            pipedrive_tap.execute_request(endpoint, api_version='v1')
        except requests.exceptions.Timeout:
            pass

        # Verify that requests.get is called 5 times
        self.assertEqual(mocked_request.call_count, 5)

@mock.patch("time.sleep")
class TestConnectionErrorBackoff(unittest.TestCase):

    @mock.patch("requests.get", side_effect = requests.exceptions.ConnectionError)
    def test_request_timeout_backoff_execute_request(self, mocked_request, mocked_sleep):
        """
            Verify execute_request function is backoff for 5 times on ConnectionError exceeption
        """
        config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc"}
        state = {}
        endpoint = 'xyz'

        # Initialize PipedriveTap object
        pipedrive_tap = _tap.PipedriveTap(config, state)
        try:
            pipedrive_tap.execute_request(endpoint, api_version='v1')
        except requests.exceptions.ConnectionError:
            pass

        # Verify that requests.get is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
