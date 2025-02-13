"""Map integration requests to services."""


def return_perspective_api_handler():
    """Lazy-load the perspective API handler."""
    from pipelines.classify_records.perspective_api.handler import (
        lambda_handler as ml_inference_perspective_api_handler,
    )

    return ml_inference_perspective_api_handler


def return_sociopolitical_handler():
    """Lazy-load the sociopolitical handler."""
    from pipelines.classify_records.sociopolitical.handler import (
        lambda_handler as ml_inference_sociopolitical_handler,
    )

    return ml_inference_sociopolitical_handler


def return_ime_handler():
    """Lazy-load the IME handler."""
    from pipelines.classify_records.ime.handler import (
        lambda_handler as ml_inference_ime_handler,
    )

    return ml_inference_ime_handler


# Map service names to their loader functions (not the actual handlers)
MAP_INTEGRATION_REQUEST_TO_SERVICE = {
    "ml_inference_perspective_api": return_perspective_api_handler,
    "ml_inference_sociopolitical": return_sociopolitical_handler,
    "ml_inference_ime": return_ime_handler,
}


def get_handler(service_name: str):
    """Get the handler for a given service name.

    Args:
        service_name: The name of the service to get the handler for.

    Returns:
        The handler function for the requested service.

    Raises:
        KeyError: If the service name is not found in the map.
    """
    handler_func = MAP_INTEGRATION_REQUEST_TO_SERVICE[service_name]
    return handler_func()
