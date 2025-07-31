class UploadValidationError(Exception):
    """Raised when an uploaded file fails validation (e.g., wrong headers, bad data)."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class DataProcessingError(Exception):
    """Raised when something goes wrong in a processing pipeline."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class AuthError(Exception):
    """Raised when authentication or access control fails."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message
