"""Tests for calculate_superposters.helper."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pandas as pd

from services.calculate_superposters import helper


def test_calculate_latest_superposters_percentile_filters_by_timestamp(monkeypatch):
    posts_df = pd.DataFrame(
        {
            "author_did": ["did:plc:a", "did:plc:a", "did:plc:b"],
            "synctimestamp": [
                "2023-12-31T00:00:00Z",
                "2024-01-02T00:00:00Z",
                "2023-12-31T00:00:00Z",
            ],
        }
    )
    captured = {}

    monkeypatch.setattr(
        helper,
        "load_latest_data",
        lambda *args, **kwargs: posts_df,
    )
    monkeypatch.setattr(helper, "calculate_lookback_datetime_str", lambda days: "2024-01-01T00:00:00Z")
    monkeypatch.setattr(helper, "generate_current_datetime_str", lambda: "2024-01-03T00:00:00Z")
    monkeypatch.setattr(helper, "_get_dynamodb", lambda: MagicMock())
    monkeypatch.setattr(
        helper,
        "export_data_to_local_storage",
        lambda *, service, df, export_format="parquet": captured.setdefault("df", df.copy()),
    )
    monkeypatch.setattr(
        helper,
        "insert_superposter_session",
        lambda *args, **kwargs: None,
    )

    helper.calculate_latest_superposters(top_n_percent=1.0, threshold=None, use_athena=False)

    assert "df" in captured
    exported = captured["df"]
    assert len(exported) == 1
    superposters = json.loads(exported.iloc[0]["superposters"])
    assert [row["author_did"] for row in superposters] == ["did:plc:a"]
