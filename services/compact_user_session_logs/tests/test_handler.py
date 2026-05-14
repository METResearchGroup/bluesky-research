"""Tests for pipeline handler wiring."""

from unittest.mock import patch

from pipelines.compact_user_session_logs.handler import lambda_handler


@patch("pipelines.compact_user_session_logs.handler.main")
def test_lambda_handler_calls_service_main(mock_main) -> None:
    result = lambda_handler(None, None)

    assert result["statusCode"] == 200
    assert "Compact user session logs completed successfully" in result["body"]
    mock_main.assert_called_once()
