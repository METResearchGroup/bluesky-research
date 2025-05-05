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


def return_preprocess_raw_data_handler():
    """Lazy-load the preprocess raw data handler."""
    from pipelines.preprocess_raw_data.handler import (
        lambda_handler as preprocess_raw_data_handler,
    )

    return preprocess_raw_data_handler


def get_handler(service_name: str):
    """Get the handler for a given service name.

    Args:
        service_name: The name of the service to get the handler for.

    Returns:
        The handler function for the requested service.

    Raises:
        KeyError: If the service name is not found in the map.
    """
    if service_name == "ml_inference_perspective_api":
        handler_func = return_perspective_api_handler
    elif service_name == "ml_inference_sociopolitical":
        handler_func = return_sociopolitical_handler
    elif service_name == "ml_inference_ime":
        handler_func = return_ime_handler
    elif service_name == "preprocess_raw_data":
        handler_func = return_preprocess_raw_data_handler
    else:
        raise KeyError(f"Unknown service name: {service_name}")
    return handler_func()
