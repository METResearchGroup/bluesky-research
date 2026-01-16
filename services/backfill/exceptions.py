class EnqueueServiceError(Exception):
    """Raised when enqueuing records fails."""

    pass


class IntegrationRunnerServiceError(Exception):
    """Raised when running integrations fails."""

    pass
