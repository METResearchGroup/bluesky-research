"""Error taxonomy for pipeline invocation."""


class PipelineInvocationError(Exception):
    """Base exception for pipeline invocation errors."""

    pass


class UnknownServiceError(PipelineInvocationError):
    """Raised when service name not found in registry.

    This is a configuration error - the service name is invalid or not registered.
    """

    def __init__(self, service_name: str, available_services: list[str]):
        self.service_name = service_name
        self.available_services = available_services
        message = (
            f"Unknown service name: {service_name}. "
            f"Registered services: {available_services}. "
            f"Use PipelineHandlerRegistry.list_services() to see all available services."
        )
        super().__init__(message)


class MetadataWriteError(PipelineInvocationError):
    """Raised when metadata write to DynamoDB fails.

    This is a critical failure - metadata tracking is essential for observability.
    The pipeline execution should fail if metadata cannot be written.
    """

    def __init__(self, service: str, original_error: Exception):
        self.service = service
        self.original_error = original_error
        message = f"Failed to write metadata to DynamoDB for service {service}: {original_error}"
        super().__init__(message)
        self.__cause__ = original_error


class ObservabilityError(PipelineInvocationError):
    """Raised when observability logging fails (non-critical).

    This includes WandB logging failures. These should not block pipeline execution
    but should be logged for monitoring.
    """

    def __init__(self, service: str, component: str, original_error: Exception):
        self.service = service
        self.component = component  # e.g., "wandb", "metrics"
        self.original_error = original_error
        message = f"Observability failure for {component} in service {service}: {original_error}"
        super().__init__(message)
        self.__cause__ = original_error
