"""Tests for integration_runner_service.py."""

from __future__ import annotations

import sys
import types
from typing import Callable
from unittest.mock import Mock, patch

import pytest

from services.backfill.exceptions import IntegrationRunnerServiceError
from services.backfill.models import (
    BackfillPeriod,
    IntegrationRunnerConfigurationPayload,
    IntegrationRunnerServicePayload,
)
from services.backfill.services.integration_runner_service import IntegrationRunnerService


def _install_fake_ml_inference_module(
    monkeypatch: pytest.MonkeyPatch,
    module_path: str,
    classify_latest_posts: Callable,
) -> None:
    """Install a lightweight fake module into sys.modules for import interception."""
    fake_module = types.ModuleType(module_path)
    fake_module.classify_latest_posts = classify_latest_posts
    monkeypatch.setitem(sys.modules, module_path, fake_module)


class TestIntegrationRunnerService__init__:
    """Tests for IntegrationRunnerService.__init__ method."""

    def test_initializes_with_empty_cache(self):
        """Test that the service initializes with an empty integration cache."""
        # Arrange & Act
        service = IntegrationRunnerService()

        # Assert
        assert service.integration_strategies == {}


class TestIntegrationRunnerService_load_integration_strategy:
    """Tests for IntegrationRunnerService._load_integration_strategy method."""

    @pytest.mark.parametrize(
        "integration_name,module_path",
        [
            (
                "ml_inference_perspective_api",
                "services.ml_inference.perspective_api.perspective_api",
            ),
            (
                "ml_inference_sociopolitical",
                "services.ml_inference.sociopolitical.sociopolitical",
            ),
            ("ml_inference_ime", "services.ml_inference.ime.ime"),
            (
                "ml_inference_valence_classifier",
                "services.ml_inference.valence_classifier.valence_classifier",
            ),
            (
                "ml_inference_intergroup",
                "services.ml_inference.intergroup.intergroup",
            ),
        ],
    )
    def test_returns_correct_callable_for_each_known_integration(
        self, monkeypatch: pytest.MonkeyPatch, integration_name: str, module_path: str
    ):
        """Test that each supported integration name returns its classify_latest_posts callable."""
        # Arrange
        service = IntegrationRunnerService()

        def expected_callable(*args, **kwargs):
            return {"args": args, "kwargs": kwargs}

        _install_fake_ml_inference_module(
            monkeypatch=monkeypatch,
            module_path=module_path,
            classify_latest_posts=expected_callable,
        )

        # Act
        result = service._load_integration_strategy(integration_name=integration_name)

        # Assert
        assert result is expected_callable

    def test_raises_integration_runner_service_error_for_invalid_name(self):
        """Test that invalid integration names raise IntegrationRunnerServiceError."""
        # Arrange
        service = IntegrationRunnerService()
        integration_name = "invalid_integration"

        # Act & Assert
        with pytest.raises(IntegrationRunnerServiceError, match="Invalid integration name"):
            service._load_integration_strategy(integration_name=integration_name)


class TestIntegrationRunnerService_get_or_load_integration_entrypoint:
    """Tests for IntegrationRunnerService._get_or_load_integration_entrypoint method."""

    def test_loads_and_caches_when_missing(self):
        """Test that the entrypoint is loaded and cached when not present."""
        # Arrange
        service = IntegrationRunnerService()
        integration_name = "ml_inference_ime"
        expected_callable = lambda **kwargs: None

        with patch.object(
            service, "_load_integration_strategy", return_value=expected_callable
        ) as mock_loader:
            # Act
            result = service._get_or_load_integration_entrypoint(
                integration_name=integration_name
            )

            # Assert
            assert result is expected_callable
            mock_loader.assert_called_once_with(integration_name)
            assert service.integration_strategies[integration_name] is expected_callable

    def test_returns_cached_callable_without_reloading(self):
        """Test that cached entrypoints are returned without calling the loader."""
        # Arrange
        service = IntegrationRunnerService()
        integration_name = "ml_inference_ime"
        expected_callable = lambda **kwargs: None
        service.integration_strategies[integration_name] = expected_callable

        with patch.object(service, "_load_integration_strategy") as mock_loader:
            # Act
            result = service._get_or_load_integration_entrypoint(
                integration_name=integration_name
            )

            # Assert
            assert result is expected_callable
            mock_loader.assert_not_called()


class TestIntegrationRunnerService_create_integration_dispatch_payload:
    """Tests for IntegrationRunnerService._create_integration_dispatch_payload method."""

    def test_creates_payload_matching_ml_inference_signature(self):
        """Test that dispatch payload matches the ML inference classify_latest_posts signature."""
        # Arrange
        service = IntegrationRunnerService()
        config_payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_ime",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=123,
        )
        expected = {
            "backfill_period": "days",
            "backfill_duration": 123,
            "run_classification": True,
            "previous_run_metadata": None,
            "event": None,
        }

        # Act
        result = service._create_integration_dispatch_payload(payload=config_payload)

        # Assert
        assert result == expected


class TestIntegrationRunnerService_run_single_integration:
    """Tests for IntegrationRunnerService._run_single_integration method."""

    def test_dispatches_to_integration_entrypoint_with_dispatch_payload(self):
        """Test that _run_single_integration calls the integration fn with dispatch payload."""
        # Arrange
        service = IntegrationRunnerService()
        config_payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_ime",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=7,
        )
        dispatch_payload = {"backfill_period": "days", "backfill_duration": 7}
        integration_fn = Mock()

        with patch.object(
            service,
            "_create_integration_dispatch_payload",
            return_value=dispatch_payload,
        ) as mock_create_payload, patch.object(
            service,
            "_get_or_load_integration_entrypoint",
            return_value=integration_fn,
        ) as mock_get_entrypoint:
            # Act
            service._run_single_integration(payload=config_payload)

            # Assert
            # The implementation calls the helper positionally.
            mock_create_payload.assert_called_once_with(config_payload)
            mock_get_entrypoint.assert_called_once_with(config_payload.integration_name)
            integration_fn.assert_called_once_with(**dispatch_payload)

    def test_wraps_exception_from_entrypoint_as_integration_runner_service_error(self):
        """Test that exceptions from the integration entrypoint are wrapped and logged."""
        # Arrange
        service = IntegrationRunnerService()
        config_payload = IntegrationRunnerConfigurationPayload(
            integration_name="ml_inference_ime",
            backfill_period=BackfillPeriod.DAYS,
            backfill_duration=None,
        )
        dispatch_payload = {"backfill_period": "days", "backfill_duration": None}
        integration_fn = Mock(side_effect=Exception("boom"))

        with patch.object(
            service,
            "_create_integration_dispatch_payload",
            return_value=dispatch_payload,
        ), patch.object(
            service,
            "_get_or_load_integration_entrypoint",
            return_value=integration_fn,
        ), patch(
            "services.backfill.services.integration_runner_service.logger"
        ) as mock_logger:
            # Act & Assert
            with pytest.raises(
                IntegrationRunnerServiceError, match="Error running integration"
            ):
                service._run_single_integration(payload=config_payload)

            mock_logger.error.assert_called_once()


class TestIntegrationRunnerService_run_integrations:
    """Tests for IntegrationRunnerService.run_integrations method."""

    def test_runs_integrations_sequentially(self):
        """Test that integrations are executed in the order provided by the payload."""
        # Arrange
        service = IntegrationRunnerService()
        configs = [
            IntegrationRunnerConfigurationPayload(
                integration_name="ml_inference_perspective_api",
                backfill_period=BackfillPeriod.DAYS,
                backfill_duration=None,
            ),
            IntegrationRunnerConfigurationPayload(
                integration_name="ml_inference_ime",
                backfill_period=BackfillPeriod.DAYS,
                backfill_duration=None,
            ),
        ]
        payload = IntegrationRunnerServicePayload(integration_configs=configs)
        call_order: list[str] = []

        def record_call(p: IntegrationRunnerConfigurationPayload) -> None:
            call_order.append(p.integration_name)

        with patch.object(service, "_run_single_integration", side_effect=record_call):
            # Act
            service.run_integrations(payload=payload)

        # Assert
        assert call_order == [
            "ml_inference_perspective_api",
            "ml_inference_ime",
        ]

    def test_logs_start_and_completion_messages(self):
        """Test that run_integrations logs start and completion messages."""
        # Arrange
        service = IntegrationRunnerService()
        configs = [
            IntegrationRunnerConfigurationPayload(
                integration_name="ml_inference_ime",
                backfill_period=BackfillPeriod.DAYS,
                backfill_duration=None,
            )
        ]
        payload = IntegrationRunnerServicePayload(integration_configs=configs)

        with patch(
            "services.backfill.services.integration_runner_service.logger"
        ) as mock_logger, patch.object(service, "_run_single_integration") as mock_run_one:
            mock_run_one.return_value = None

            # Act
            service.run_integrations(payload=payload)

            # Assert
            info_messages = [str(call) for call in mock_logger.info.call_args_list]
            assert any("Running integrations" in msg for msg in info_messages)
            assert any("completed successfully" in msg for msg in info_messages)

    def test_wraps_exception_from_single_integration(self):
        """Test that exceptions from _run_single_integration are wrapped and logged."""
        # Arrange
        service = IntegrationRunnerService()
        configs = [
            IntegrationRunnerConfigurationPayload(
                integration_name="ml_inference_ime",
                backfill_period=BackfillPeriod.DAYS,
                backfill_duration=None,
            )
        ]
        payload = IntegrationRunnerServicePayload(integration_configs=configs)

        with patch(
            "services.backfill.services.integration_runner_service.logger"
        ) as mock_logger, patch.object(
            service, "_run_single_integration", side_effect=Exception("boom")
        ):
            # Act & Assert
            with pytest.raises(
                IntegrationRunnerServiceError, match="Error running integrations"
            ):
                service.run_integrations(payload=payload)

            mock_logger.error.assert_called_once()
