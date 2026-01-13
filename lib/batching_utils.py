"""Batching utilities."""

from logging import Logger


def create_batches(batch_list, batch_size) -> list[list]:
    """Create batches of a given size.

    NOTE: if batching becomes a problem due to, for example, keeping things
    in memory, can move to yielding instead. Loading everything in memory
    was easier to iterate first, but can investigate things like yields.
    """
    batches: list[list] = []
    for i in range(0, len(batch_list), batch_size):
        batches.append(batch_list[i : i + batch_size])
    return batches


def update_batching_progress(
    batch_index: int,
    batch_interval: int,
    total_batches: int,
    logger: Logger,
) -> None:
    """Update batching progress."""
    if total_batches <= 0:
        # nothing to do here, but also not necessarily an error
        # (can imagine, for example a case of no batches to process)
        # (we want other functions to validate total_batches, not this one)
        return
    if batch_index % batch_interval == 0:
        total_progress_percentage: int = int((batch_index / total_batches) * 100)
        logger.info(
            f"[Completion percentage: {total_progress_percentage}%] Processing batch {batch_index}/{total_batches}..."
        )
