

class PipedriveError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

# Reference: https://pipedrive.readme.io/docs/core-api-concepts-http-status-codes
class Pipedrive5xxError(PipedriveError):
    pass

class PipedriveNotFoundError(PipedriveError):
    pass

class PipedriveBadRequestError(PipedriveError):
    pass

class PipedriveUnauthorizedError(PipedriveError):
    pass

class PipedrivePaymentRequiredError(PipedriveError):
    pass

class PipedriveForbiddenError(PipedriveError):
    pass

class PipedriveGoneError(PipedriveError):
    pass

class PipedriveUnsupportedMediaError(PipedriveError):
    pass

class PipedriveUnprocessableEntityError(PipedriveError):
    pass

class PipedriveTooManyRequestsError(PipedriveError):
    pass

class PipedriveTooManyRequestsInSecondError(PipedriveError):
    pass

class PipedriveInternalServiceError(Pipedrive5xxError): # 500 error
    pass

class PipedriveNotImplementedError(Pipedrive5xxError): # 501 error
    pass

class PipedriveServiceUnavailableError(Pipedrive5xxError): # 503 error
    pass
