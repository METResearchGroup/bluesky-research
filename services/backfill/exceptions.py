class EnqueueServiceError(Exception):
    """Raised when enqueuing records fails."""

    pass


class IntegrationRunnerServiceError(Exception):
    """Raised when running integrations fails."""

    pass


class BackfillDataAdapterError(Exception):
    """Raised when a backfill data adapter fails."""

    pass
