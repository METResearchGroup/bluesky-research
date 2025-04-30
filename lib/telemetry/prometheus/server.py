"""Setup for Prometheus server."""

from prometheus_client import start_http_server


def start_metrics_server(port: int = 8000) -> None:
    """
    Start the Prometheus /metrics server on the given port.
    Should be called once at service startup.
    """
    start_http_server(port)
