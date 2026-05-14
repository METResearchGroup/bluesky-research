"""Ensure participant_data users load is mocked before helper module import."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

_patcher: Any = None


def pytest_configure(config):  # noqa: ARG001
    global _patcher
    user = MagicMock()
    user.bluesky_user_did = "did:plc:testauthor"
    user.bluesky_handle = "@test.handle"
    _patcher = patch(
        "services.participant_data.helper.get_all_users",
        return_value=[user],
    )
    _patcher.start()


def pytest_unconfigure(config):  # noqa: ARG001
    global _patcher
    if _patcher is not None:
        _patcher.stop()
        _patcher = None
