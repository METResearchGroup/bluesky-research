"""Integration-to-prefix mapping for migrate-to-S3 backfill step."""

from scripts.migrate_research_data_to_s3.constants import PREFIXES_TO_MIGRATE


def prefixes_for_integrations(integration_names: list[str]) -> list[str]:
    """Return PREFIXES_TO_MIGRATE entries that match any of the given integration names.

    A prefix matches if it equals an integration name or starts with "{name}/".
    Used by the backfill CLI to migrate only integration-scoped data (e.g. ml_inference_*).

    Args:
        integration_names: Full integration names (e.g. ["ml_inference_intergroup", "ml_inference_ime"]).
            Abbreviations are not resolved here; the caller must use resolved names.

    Returns:
        Sorted list of matching prefixes (e.g. ["ml_inference_intergroup/active", "ml_inference_intergroup/cache", ...]).
    """
    if not integration_names:
        return []
    result: list[str] = []
    for p in PREFIXES_TO_MIGRATE:
        for name in integration_names:
            if p == name or p.startswith(f"{name}/"):
                result.append(p)
                break
    return sorted(result)
