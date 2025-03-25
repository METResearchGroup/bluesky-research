"""Helper utilities for managing backfill-specific session metadata.

Separate from api/integrations_router/run.py, because that manages session
metadata for integrations instead of backfills (though TBH there's an argument
for refactoring that to make it more consistent).
"""

from lib.metadata.models import RunExecutionMetadata


def load_latest_backfill_session_metadata() -> RunExecutionMetadata:
    pass


def write_backfill_metadata_to_db(backfill_metadata: RunExecutionMetadata) -> None:
    pass
