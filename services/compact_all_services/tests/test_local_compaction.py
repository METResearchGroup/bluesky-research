"""Tests for local compaction."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from services.compact_all_services.local_compaction import (
    LOCAL_COMPACTION_SERVICE_NAMES,
    compact_all_local_services,
    compact_local_service,
)


def test_local_compaction_service_list_matches_snapshot() -> None:
    assert LOCAL_COMPACTION_SERVICE_NAMES == (
        "preprocessed_posts",
        "in_network_user_activity",
        "scraped_user_social_network",
        "study_user_activity",
        "study_user_likes",
        "study_user_like_on_user_post",
        "sync_most_liked_posts",
        "daily_superposters",
        "user_session_logs",
        "feed_analytics",
        "post_scores",
        "consolidated_enriched_post_records",
        "ml_inference_sociopolitical",
        "ml_inference_perspective_api",
    )


def test_compact_local_service_default_exports_with_lookback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame({"a": [1]})
    load_mock = MagicMock(return_value=df)
    list_mock = MagicMock(return_value=["f1.parquet"])
    export_mock = MagicMock()
    delete_mock = MagicMock()
    empty_dirs_mock = MagicMock()

    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.load_data_from_local_storage",
        load_mock,
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.list_filenames",
        list_mock,
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.export_data_to_local_storage",
        export_mock,
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_files",
        delete_mock,
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_empty_folders_for_service",
        empty_dirs_mock,
    )

    compact_local_service("feed_analytics", delete_old_files=True)

    load_mock.assert_called_once_with("feed_analytics")
    list_mock.assert_called_once_with("feed_analytics")
    export_mock.assert_called_once()
    _, kwargs = export_mock.call_args
    assert kwargs["service"] == "feed_analytics"
    assert kwargs["df"] is df
    assert kwargs["export_format"] == "parquet"
    delete_mock.assert_called_once_with(["f1.parquet"])
    empty_dirs_mock.assert_called_once_with("feed_analytics")


def test_compact_local_service_preprocessed_splits_by_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame(
        {
            "source": ["firehose", "most_liked"],
            "x": [1, 2],
        }
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.load_data_from_local_storage",
        MagicMock(return_value=df),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.list_filenames",
        MagicMock(return_value=["a"]),
    )
    export_mock = MagicMock()
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.export_data_to_local_storage",
        export_mock,
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_files",
        MagicMock(),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_empty_folders_for_service",
        MagicMock(),
    )

    compact_local_service("preprocessed_posts")

    assert export_mock.call_count == 2
    by_source = {
        call.kwargs["custom_args"]["source"]: call.kwargs["df"]
        for call in export_mock.call_args_list
    }
    assert list(by_source["firehose"]["x"]) == [1]
    assert list(by_source["most_liked"]["x"]) == [2]


def test_compact_local_service_study_user_activity_record_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame({"k": [1]})
    export_mock = MagicMock()
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.load_data_from_local_storage",
        MagicMock(return_value=df),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.list_filenames",
        MagicMock(return_value=[]),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.export_data_to_local_storage",
        export_mock,
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_files",
        MagicMock(),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_empty_folders_for_service",
        MagicMock(),
    )

    compact_local_service("study_user_activity")

    export_mock.assert_called_once_with(
        service="study_user_activity", df=df, custom_args={"record_type": "post"}
    )


def test_compact_local_service_empty_ml_like_skips_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame()
    export_mock = MagicMock()
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.load_data_from_local_storage",
        MagicMock(return_value=df),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.list_filenames",
        MagicMock(return_value=[]),
    )
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.export_data_to_local_storage",
        export_mock,
    )
    delete_mock = MagicMock()
    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.delete_files",
        delete_mock,
    )

    compact_local_service("ml_inference_sociopolitical")

    export_mock.assert_not_called()
    delete_mock.assert_not_called()


def test_compact_all_invokes_each_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_compact(service: str, **kwargs: object) -> None:
        calls.append(service)

    monkeypatch.setattr(
        "services.compact_all_services.local_compaction.compact_local_service",
        fake_compact,
    )

    compact_all_local_services()

    assert calls == list(LOCAL_COMPACTION_SERVICE_NAMES)
