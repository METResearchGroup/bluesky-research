"""Custom exceptions for feed storage operations."""


class StorageError(Exception):
    """Exception raised when storage operations fail.

    This exception is raised when any storage operation (read, write, TTL, etc.)
    encounters an error. It provides a consistent error interface across all
    storage adapters.

    The original exception is automatically preserved via Python's exception
    chaining when raised with 'raise StorageError(...) from e'.
    """

    pass
