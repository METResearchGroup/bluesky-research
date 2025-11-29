"""View migration tracker database status per prefix."""

from scripts.migrate_research_data_to_s3.constants import PREFIXES_TO_MIGRATE
from scripts.migrate_research_data_to_s3.migration_tracker import MigrationTracker

migration_tracker_db = MigrationTracker()


def print_prefix_status_table(prefix: str) -> None:
    """Print a status table for a specific prefix.

    Args:
        prefix: The prefix to show status for.
    """
    status_counts = migration_tracker_db.get_status_counts_for_prefix(prefix)

    print(f"\n=== {prefix} ===")
    print("\nStatus Counts:")
    print("-" * 40)

    # Print status counts in a table format
    status_order = ["pending", "in_progress", "completed", "failed", "skipped"]
    emoji_map = {
        "pending": "⏳",
        "in_progress": "🔄",
        "completed": "✅",
        "failed": "❌",
        "skipped": "⏭️",
    }

    for status in status_order:
        count = status_counts.get(status, 0)
        emoji = emoji_map.get(status, "")
        print(f"  {emoji} {status.capitalize()}: {count}")

    # Calculate total and progress
    total = sum(status_counts.values())
    completed = status_counts.get("completed", 0)

    if total > 0:
        progress_pct = (completed / total) * 100
        print(f"\n  Total: {total}")
        print(f"  Progress: {progress_pct:.1f}%")
    else:
        print("\n  No files found for this prefix")

    print("-" * 40)


def view_migration_status() -> None:
    """View migration status per prefix and overall summary."""
    print("\n" + "=" * 60)
    print("MIGRATION STATUS BY PREFIX")
    print("=" * 60)

    # Print status for each prefix
    for prefix in PREFIXES_TO_MIGRATE:
        print_prefix_status_table(prefix)

    # Print overall checklist
    print("\n" + "=" * 60)
    print("OVERALL MIGRATION SUMMARY")
    print("=" * 60)
    migration_tracker_db.print_checklist()


if __name__ == "__main__":
    view_migration_status()
