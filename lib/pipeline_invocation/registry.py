"""Pipeline handler registry for service discovery."""

from typing import Callable

from lib.metadata.models import RunExecutionMetadata


# Registry implementation
class PipelineHandlerRegistry:
    """Registry for pipeline handlers with lazy loading.

    Uses factory functions to enable lazy loading of handlers, avoiding
    circular imports and reducing initial import time.
    """

    _handlers: dict[str, Callable[[], Callable[..., RunExecutionMetadata]]] = {}

    @classmethod
    def register(
        cls, service_name: str
    ) -> Callable[[Callable[[], Callable]], Callable]:
        """Register a handler factory for a service name.

        The factory function is called lazily when get_handler() is invoked,
        enabling lazy loading of handler modules to avoid circular imports.

        Usage:
            @PipelineHandlerRegistry.register("ml_inference_perspective_api")
            def _get_perspective_handler():
                from pipelines.classify_records.perspective_api.handler import lambda_handler
                return lambda_handler

        Args:
            service_name: The service name to register (e.g., "ml_inference_perspective_api")

        Returns:
            Decorator function that registers the factory
        """

        def decorator(factory: Callable[[], Callable]) -> Callable:
            cls._handlers[service_name] = factory
            return factory

        return decorator

    @classmethod
    def get_handler(cls, service_name: str) -> Callable[..., RunExecutionMetadata]:
        """Get the handler for a given service name.

        The factory function is called to lazily load and return the handler.

        Args:
            service_name: The name of the service to get the handler for.

        Returns:
            The handler function for the requested service.

        Raises:
            KeyError: If the service name is not found in the registry.
                The error message includes available service names.
        """
        factory = cls._handlers.get(service_name)
        if factory is None:
            available = list(cls._handlers.keys())
            raise KeyError(
                f"Unknown service name: {service_name}. "
                f"Registered services: {available}. "
                f"Use PipelineHandlerRegistry.list_services() to see all available services."
            )
        return factory()  # Call factory to get handler

    @classmethod
    def list_services(cls) -> list[str]:
        """List all registered service names.

        Returns:
            List of all registered service names, sorted alphabetically.
        """
        return sorted(cls._handlers.keys())

    @classmethod
    def inject_handler_for_testing(cls, service_name: str, handler: Callable) -> None:
        """Inject a mock handler for testing purposes.

        This temporarily replaces the factory with a direct handler return.
        Use reset_test_handlers() to restore original behavior.

        Args:
            service_name: Service name to override
            handler: Handler function to use instead of factory

        Raises:
            KeyError: If the service name is not registered
        """
        if service_name not in cls._handlers:
            available = list(cls._handlers.keys())
            raise KeyError(
                f"Service {service_name} not registered. "
                f"Available services: {available}"
            )

        # Store original if not already stored
        if not hasattr(cls, "_test_originals"):
            cls._test_originals = {}
        if service_name not in cls._test_originals:
            cls._test_originals[service_name] = cls._handlers[service_name]

        # Replace with a factory that returns the mock handler
        cls._handlers[service_name] = lambda: handler

    @classmethod
    def reset_test_handlers(cls) -> None:
        """Reset handlers to original implementations after testing."""
        if hasattr(cls, "_test_originals"):
            cls._handlers.update(cls._test_originals)
            cls._test_originals = {}


# Register all handlers
@PipelineHandlerRegistry.register("ml_inference_perspective_api")
def _get_perspective_api_handler():
    """Lazy-load the perspective API handler."""
    from pipelines.classify_records.perspective_api.handler import lambda_handler

    return lambda_handler


@PipelineHandlerRegistry.register("ml_inference_sociopolitical")
def _get_sociopolitical_handler():
    """Lazy-load the sociopolitical handler."""
    from pipelines.classify_records.sociopolitical.handler import lambda_handler

    return lambda_handler


@PipelineHandlerRegistry.register("ml_inference_ime")
def _get_ime_handler():
    """Lazy-load the IME handler."""
    from pipelines.classify_records.ime.handler import lambda_handler

    return lambda_handler


@PipelineHandlerRegistry.register("ml_inference_valence_classifier")
def _get_valence_classifier_handler():
    """Lazy-load the valence classifier handler."""
    from pipelines.classify_records.valence_classifier.handler import lambda_handler

    return lambda_handler


@PipelineHandlerRegistry.register("preprocess_raw_data")
def _get_preprocess_raw_data_handler():
    """Lazy-load the preprocess raw data handler."""
    from pipelines.preprocess_raw_data.handler import lambda_handler

    return lambda_handler
