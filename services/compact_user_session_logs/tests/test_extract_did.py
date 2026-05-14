"""Tests for DID extraction from CloudWatch log lines."""

from services.compact_user_session_logs.get_missing_user_session_logs_cloudwatch import (
    extract_did,
)


def test_extract_did_returns_plc_did() -> None:
    msg = "User DID not in the study: did:plc:abc123xyz something else"
    assert extract_did(msg) == "did:plc:abc123xyz"


def test_extract_did_returns_none_when_no_match() -> None:
    assert extract_did("no did here") is None
