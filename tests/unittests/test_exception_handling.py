import tap_pipedrive.tap as _tap
import unittest
import requests
import simplejson
from unittest import mock

class Mockresponse:
    def __init__(self, status_code, resp=None, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("sample message")

def get_mock_http_response(status_code, contents):
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

@mock.patch('time.sleep')
@mock.patch('requests.get')
class TestExecuteRequestExceptionHandling(unittest.TestCase):
    """
    Test cases to verify if the exceptions are handled as expected while communicating with Pipedrive Environment 
    """

    # config
    config = {"start_date":"2017-01-01T00:00:00Z","api_token":"abc"}
    endpoint = "xyz"

    # pipedrive object
    pipedrive_tap = _tap.PipedriveTap(config, {})

    def test_json_decode_successfull_with_200(self, mocked_jsondecode_successful_request, mocked_sleep):
        json_decode_str = '{"success": true, "data" : []}'
        mocked_jsondecode_successful_request.return_value = get_mock_http_response(200, json_decode_str)

        self.pipedrive_tap.execute_request(self.endpoint, "v1")

        self.assertEqual(mocked_jsondecode_successful_request.call_count, 1)

    def test_json_decode_exception(self, mocked_jsondecode_failing_request, mocked_sleep):
        json_decode_error_str = '{\Currency\': \'value\'}'
        mocked_jsondecode_failing_request.return_value = get_mock_http_response(200, json_decode_error_str)

        with self.assertRaises(simplejson.scanner.JSONDecodeError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        self.assertEqual(mocked_jsondecode_failing_request.call_count, 3)

    def test_badrequest_400_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(400, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveBadRequestError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 400, Error: Request is missing or has a bad parameter."
        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_unauthorized_401_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(401, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveUnauthorizedError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_paymentrequired_402_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(402, {}, raise_error=True)

        with self.assertRaises(_tap.PipedrivePaymentRequiredError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 402, Error: Company account is not open (possible reason: trial expired, payment details not entered)."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_forbidden_403_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(403, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveForbiddenError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 403, Error: Invalid authorization credentials or permissions."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_notfound_404_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(404, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveNotFoundError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 404, Error: The requested resource does not exist."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_gone_410_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(410, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveGoneError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 410, Error: The old resource is permanently unavailable."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_unsupported_media_type_415_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(415, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveUnsupportedMediaError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 415, Error: The feature is not enabled."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_unproceesable_entity_422_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(422, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveUnprocessableEntityError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 422, Error: Webhook limit reached."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_rate_limit_in_seconds_429_error(self, mocked_request, mocked_sleep):
        headers = {"X-RateLimit-Reset": 2, "X-RateLimit-Remaining":0}
        mocked_request.return_value = Mockresponse(429, {}, headers=headers, raise_error=True)

        with self.assertRaises(_tap.PipedriveTooManyRequestsInSecondError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 429, Error: Rate limit has been exceeded. Please retry after 2 seconds."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 3)

    def test_rate_limit_in_day_429_error(self, mocked_request, mocked_sleep):
        headers = {"X-RateLimit-Reset": 200, "X-RateLimit-Remaining":10}
        mocked_request.return_value = Mockresponse(429, {}, headers=headers, raise_error=True)

        with self.assertRaises(_tap.PipedriveTooManyRequestsError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 429, Error: Daily Rate limit has been exceeded. Please retry after 200 seconds."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 1)

    def test_internalservererror_500_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(500, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveInternalServiceError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 500, Error: Internal Service Error from PipeDrive."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 5)

    def test_not_implemented_501_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(501, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveNotImplementedError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 501, Error: Functionality does not exist."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 5)

    def test_service_unavailable_503_error(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(503, {}, raise_error=True)

        with self.assertRaises(_tap.PipedriveServiceUnavailableError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 503, Error: Schedule maintenance on Pipedrive's end."

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 5)

    def test_5XX_errors_other_than_doc(self, mocked_request, mocked_sleep):
        mocked_request.return_value = Mockresponse(524, {}, raise_error=True)

        with self.assertRaises(_tap.Pipedrive5xxError) as e:
            self.pipedrive_tap.execute_request(self.endpoint, "v1")

        expected_error_message = "HTTP-error-code: 524, Error: Unknown Error"

        # Verifying the message formed for the custom exception
        self.assertEquals(str(e.exception), expected_error_message)

        self.assertEqual(mocked_request.call_count, 5)
