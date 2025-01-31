# Integrations Router

Contains API logic for managing integrations. This is a centralized API that takes as input requests to integrations and routes them to the appropriate integration service.

## Overview

The Integrations Router provides a unified interface for routing integration requests to different services. It handles:

- Request validation and parsing
- Service routing
- Execution tracking
- Response formatting
- Error handling

## API Interface

### Main Entry Point

The main entry point is the `route_and_run_integration_request()` function which takes a dictionary request and returns an `IntegrationResponse`.

### Request Format

Requests must be JSON objects with the following structure:

```json
{
    "service": "string",  // Name of the service to call
    "payload": {          // Service-specific payload
        "run_type": "prod" | "backfill",
        "payload": object,
        "metadata": object
    },
    "metadata": object    // Additional request metadata
}
```

### Response Format 

Responses have the following structure:

```json
{
    "service": "string",      // Name of service that was called
    "timestamp": "string",    // ISO timestamp of execution
    "status_code": number,    // HTTP status code
    "body": "string"         // Response body
}
```

## Supported Services

Currently supported integration services:

- `ml_inference_perspective_api`: Perspective API ML inference service
  - Handles classification of content using Google's Perspective API
  - Supports both production and backfill runs

## Example Usage

### ML Inference Request

```python
request = {
    "service": "ml_inference_perspective_api",
    "payload": {
        "run_type": "prod",
        "payload": {
            "backfill_period": None,
            "backfill_duration": None
        },
        "metadata": {}
    },
    "metadata": {}
}

response = route_and_run_integration_request(request)
```

### Example Response

```python
{
    "service": "ml_inference_perspective_api",
    "timestamp": "2024-03-14T12:00:00Z",
    "status_code": 200,
    "body": "Classification of latest posts completed successfully"
}
```

## Error Handling

The router provides comprehensive error handling:

- Invalid requests return validation errors
- Service execution errors are caught and logged
- All errors are tracked in DynamoDB
- Failed requests return appropriate error status codes and messages

## Execution Tracking

All integration runs are tracked in DynamoDB with:

- Service name
- Timestamp
- Status code
- Response body
- Additional metadata

## Adding New Services

To add a new service:

1. Create the service handler function
2. Add the service to `MAP_INTEGRATION_REQUEST_TO_SERVICE` in map.py
3. Ensure the handler returns metadata matching the `RunExecutionMetadata` model

```python
def route_and_run_integration_request(request: dict) -> IntegrationResponse:
```
