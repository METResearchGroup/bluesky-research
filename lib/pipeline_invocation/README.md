# Pipeline Invocation Library

This library provides a unified interface for invoking pipeline handlers with built-in observability, metadata tracking, and error handling.

## Overview

The pipeline invocation library simplifies the process of calling various pipeline handlers (e.g., ML inference services, data preprocessing pipelines) by providing:

- **Registry Pattern**: Service discovery and lazy loading of handlers
- **Dependency Injection**: Testable components with injectable dependencies
- **Observability**: Automatic metadata tracking and WandB logging
- **Error Handling**: Custom error taxonomy for clear error messages

## Architecture

```
┌─────────────────────────────────┐
│  Services/Scripts               │
│  (callers)                      │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  invoker.py                     │
│  invoke_pipeline_handler()      │
│  (public API)                   │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  executor.py                    │
│  run_integration_request()      │
│  (with observability)           │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  registry.py                    │
│  PipelineHandlerRegistry        │
│  (handler discovery)            │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  Pipeline Handlers              │
│  (actual implementation)        │
└─────────────────────────────────┘
```

## Usage

### Basic Usage

```python
from lib.pipeline_invocation import invoke_pipeline_handler

# Invoke a pipeline handler
result = invoke_pipeline_handler(
    service="ml_inference_perspective_api",
    payload={"run_type": "backfill"},
    request_metadata={"request_id": "123"}
)

print(f"Status: {result.status_code}")
print(f"Service: {result.service}")
```

### Advanced Usage with Dependency Injection

For testing or custom implementations:

```python
from lib.pipeline_invocation.executor import run_integration_request
from lib.pipeline_invocation.models import IntegrationRequest

def custom_wandb_logger(service: str, metadata: dict) -> None:
    # Custom logging implementation
    pass

def custom_metadata_loader(service: str) -> dict:
    # Custom metadata loading
    return {}

def custom_metadata_writer(metadata: RunExecutionMetadata) -> None:
    # Custom metadata writing
    pass

request = IntegrationRequest(
    service="ml_inference_perspective_api",
    payload={"run_type": "backfill"},
    metadata={}
)

result = run_integration_request(
    request,
    wandb_logger=custom_wandb_logger,
    load_metadata=custom_metadata_loader,
    write_metadata=custom_metadata_writer
)
```

## Registering New Handlers

Handlers are registered using the `@PipelineHandlerRegistry.register` decorator:

```python
from lib.pipeline_invocation.registry import PipelineHandlerRegistry

@PipelineHandlerRegistry.register("my_service")
def _get_my_service_handler():
    from pipelines.my_pipeline.handler import lambda_handler
    return lambda_handler
```

## Error Handling

The library provides custom exceptions for better error handling:

- `UnknownServiceError`: Raised when a service is not registered
- `MetadataWriteError`: Raised when metadata writing fails
- `ObservabilityError`: Raised when observability operations fail

```python
from lib.pipeline_invocation.errors import UnknownServiceError

try:
    result = invoke_pipeline_handler(service="unknown_service", payload={})
except UnknownServiceError as e:
    print(f"Unknown service: {e.service_name}")
    print(f"Available services: {e.available_services}")
```

## Models

- `IntegrationRequest`: Request model for pipeline invocation
- `IntegrationPayload`: Payload model (for backward compatibility)
- `RunExecutionMetadata`: Metadata model for execution results

## Constants

- `dynamodb_table_name`: Default DynamoDB table name for metadata storage

## Testing

The library is designed with testability in mind through dependency injection. See `tests/` directory for examples.

## Migration Notes

This library was migrated from `api/integrations_router/` to provide clearer organization and better separation of concerns. The old API had unnecessary indirection; this library provides direct, testable pipeline invocation. See [this PR](https://github.com/METResearchGroup/bluesky-research/issues/227).
