"""Public API for pipeline invocation - clean interface without DI parameters."""

from typing import Any

from lib.metadata.models import RunExecutionMetadata
from lib.pipeline_invocation.errors import UnknownServiceError
from lib.pipeline_invocation.executor import run_integration_request
from lib.pipeline_invocation.models import IntegrationRequest
from lib.pipeline_invocation.registry import PipelineHandlerRegistry


def parse_integration_request(request: dict) -> IntegrationRequest:
    """Parses an integration request into the expected format.

    Currently, the request is expected to be a JSON object with the following keys:
    - service: str
    - payload: dict
    - metadata: dict

    Kept generic in case we want to add more fields in the future.
    """
    return IntegrationRequest(**request)


def invoke_pipeline_handler(
    service: str,
    payload: dict[str, Any],
    request_metadata: dict[str, Any] | None = None,
) -> RunExecutionMetadata:
    """Invokes a pipeline handler for the given service with observability.

    This is the main public entry point for invoking pipeline handlers. It handles:
    - Handler discovery via registry pattern
    - Metadata tracking (DynamoDB)
    - WandB logging for ML experiment tracking
    - Error handling and propagation

    Args:
        service: Name of the service/pipeline to invoke (e.g., "ml_inference_perspective_api")
        payload: Service-specific payload dictionary. Common fields:
            - `run_type`: "prod" | "backfill"
            - `backfill_period`: Optional time period for backfill runs
            - Service-specific fields as needed
        request_metadata: Optional metadata dictionary for the request (e.g., request ID, user context).
            Renamed from `metadata` to avoid confusion with `RunExecutionMetadata` return type.

    Returns:
        RunExecutionMetadata: Execution metadata from the handler including:
            - `service`: Service name that was invoked
            - `timestamp`: ISO timestamp of execution
            - `status_code`: HTTP status code (if applicable)
            - `body`: Response body (if applicable)
            - `metadata_table_name`: DynamoDB table name for metadata

    Raises:
        UnknownServiceError: If the service name is not found in the handler registry.
            Use `PipelineHandlerRegistry.list_services()` to see available services.
        MetadataWriteError: If metadata write to DynamoDB fails (critical failure).
        Exception: Handler-specific errors are propagated to the caller.

    Example:
        >>> from lib.pipeline_invocation.invoker import invoke_pipeline_handler
        >>>
        >>> metadata = invoke_pipeline_handler(
        ...     service="ml_inference_perspective_api",
        ...     payload={"run_type": "backfill", "backfill_period": "2024-01-01"},
        ...     request_metadata={"request_id": "req-123"}
        ... )
        >>> print(metadata.service)
        'ml_inference_perspective_api'

    Note:
        For testing with dependency injection, use `run_integration_request()` directly
        (internal API) which accepts DI parameters.
    """
    request = IntegrationRequest(
        service=service, payload=payload, metadata=request_metadata or {}
    )

    # Convert KeyError to UnknownServiceError for better error messages
    try:
        return run_integration_request(
            request
        )  # DI happens internally, not exposed in public API
    except KeyError as e:
        available = PipelineHandlerRegistry.list_services()
        raise UnknownServiceError(service, available) from e
