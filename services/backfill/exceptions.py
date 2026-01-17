class EnqueueServiceError(Exception):
    """Raised when enqueuing records fails."""

    pass


class IntegrationRunnerServiceError(Exception):
    """Raised when running integrations fails."""

    pass


class BackfillDataAdapterError(Exception):
    """Raised when a backfill data adapter fails."""

    pass


class QueueManagerServiceError(Exception):
    """Raised when a queue manager service fails."""

    pass


class CacheBufferWriterServiceError(Exception):
    """Raised when a cache buffer writer service fails."""

    pass
