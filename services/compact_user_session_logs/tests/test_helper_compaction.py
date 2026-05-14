"""Tests for user session log compaction orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import uuid

import pandas as pd

from services.compact_user_session_logs import helper


@patch("services.compact_user_session_logs.helper.get_missing_user_session_logs_cloudwatch")
@patch("services.compact_user_session_logs.helper.load_all_user_session_logs_to_df")
@patch("services.compact_user_session_logs.helper.glue")
@patch("services.compact_user_session_logs.helper.s3")
def test_main_skips_compaction_when_one_object_per_partition(
    mock_s3: MagicMock,
    mock_glue: MagicMock,
    mock_load_df: MagicMock,
    mock_cw: MagicMock,
) -> None:
    mock_s3.list_immediate_subfolders.return_value = ["partition_date=2024-01-01/"]
    mock_s3.list_keys_given_prefix.return_value = ["only-key"]
    mock_load_df.return_value = pd.DataFrame(
        {
            "partition_date": ["2024-01-01"],
            "user_did": ["did:plc:x"],
            "timestamp": ["t"],
            "cursor": ["c"],
        }
    )

    helper.main()

    mock_s3.write_dicts_jsonl_to_s3.assert_not_called()
    mock_s3.delete_from_s3.assert_not_called()


@patch("services.compact_user_session_logs.helper.get_missing_user_session_logs_cloudwatch")
@patch("services.compact_user_session_logs.helper.load_all_user_session_logs_to_df")
@patch("services.compact_user_session_logs.helper.glue")
@patch("services.compact_user_session_logs.helper.s3")
@patch(
    "services.compact_user_session_logs.helper.uuid4",
    return_value=uuid.UUID("00000000-1111-2222-3333-444444444444"),
)
def test_main_compacts_partition_dedupes_and_deletes_sources(
    _mock_uuid: MagicMock,
    mock_s3: MagicMock,
    mock_glue: MagicMock,
    mock_load_df: MagicMock,
    mock_cw: MagicMock,
) -> None:
    mock_s3.list_immediate_subfolders.return_value = ["partition_date=2024-01-01/"]
    mock_s3.list_keys_given_prefix.return_value = ["k/old1.jsonl", "k/old2.jsonl"]
    mock_load_df.return_value = pd.DataFrame(
        {
            "partition_date": ["2024-01-01"] * 3,
            "user_did": ["did:plc:a", "did:plc:a", "did:plc:b"],
            "timestamp": ["t1", "t1", "t2"],
            "cursor": ["c1", "c1", "c2"],
        }
    )

    helper.main()

    mock_s3.write_dicts_jsonl_to_s3.assert_called_once()
    call_kw = mock_s3.write_dicts_jsonl_to_s3.call_args.kwargs
    written = call_kw["data"]
    assert len(written) == 2
    assert all("partition_date" not in row for row in written)
    keys_deleted = [c.kwargs["key"] for c in mock_s3.delete_from_s3.call_args_list]
    assert keys_deleted == ["k/old1.jsonl", "k/old2.jsonl"]

    assert mock_glue.start_crawler.call_count >= 1
