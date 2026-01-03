"""Custom exceptions for feed storage operations."""


class StorageError(Exception):
    """Exception raised when storage operations fail.

    This exception is raised when any storage operation (read, write, TTL, etc.)
    encounters an error. It provides a consistent error interface across all
    storage adapters.

    Attributes:
        message: Human-readable error message describing what went wrong
        original_error: Optional original exception that caused this error
    """

    def __init__(self, message: str, original_error: Exception | None = None):
        """Initialize StorageError.

        Args:
            message: Human-readable error message
            original_error: Optional original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.original_error = original_error
