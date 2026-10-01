"""Fabric API errors without credential or request-body contents."""


class FabricError(Exception):
    def __init__(
        self, message: str, *, status_code: int | None = None,
        request_id: str | None = None, retry_after_seconds: int | None = None,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.request_id = request_id
        self.retry_after_seconds = retry_after_seconds
        self.error_code = error_code


class FabricAuthenticationError(FabricError):
    pass


class FabricNotFoundError(FabricError):
    pass


class FabricPermissionError(FabricError):
    pass


class FabricRateLimitError(FabricError):
    pass


class FabricJobSubmissionError(FabricError):
    pass


class FabricResponseError(FabricError):
    pass
