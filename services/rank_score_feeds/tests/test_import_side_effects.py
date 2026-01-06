"""Regression tests for import-time side effects.

Issue #267: `rank_score_feeds` imports `manual_excludelist` for CSV-based exclusions.
That module must remain safe to import without importing `transform.bluesky_helper`
and triggering any Bluesky client initialization.
"""

import importlib
import sys


def test_manual_excludelist_import_does_not_import_bluesky_helper():
    # Ensure a clean slate for this assertion within the pytest process.
    sys.modules.pop(
        "services.preprocess_raw_data.classify_nsfw_content.manual_excludelist", None
    )
    sys.modules.pop("transform.bluesky_helper", None)

    importlib.import_module(
        "services.preprocess_raw_data.classify_nsfw_content.manual_excludelist"
    )

    assert "transform.bluesky_helper" not in sys.modules

