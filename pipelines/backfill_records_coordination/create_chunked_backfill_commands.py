"""Script to create the Python commands for backfilling records
(specifically records used in feeds) in chunks.

We can't do this backfill all at once, so we do it every X days.
"""

from lib.helper import get_partition_dates
from services.backfill.posts.load_data import INTEGRATIONS_LIST

start_date = "2024-10-01"
end_date = "2024-12-01"
exclude_partition_dates = ["2024-10-08"]
default_chunk_size = 3

partition_dates = get_partition_dates(start_date, end_date, exclude_partition_dates)


def generate_start_end_dates_for_chunk(
    partition_dates: list[str], chunk_size: int
) -> list[tuple[str, str]]:
    """Generate start and end dates for a chunk of partition dates."""
    res: list[tuple[str, str]] = []
    for i in range(0, len(partition_dates), chunk_size):
        max_index = min(i + chunk_size - 1, len(partition_dates) - 1)
        res.append((partition_dates[i], partition_dates[max_index]))
    return res


def main():
    # 1. For each date chunk, insert into the queues for each integration.
    for start_date, end_date in generate_start_end_dates_for_chunk(
        partition_dates, default_chunk_size
    ):
        print(
            f"python app.py --record-type posts_used_in_feeds --add-to-queue --start-date {start_date} --end-date {end_date} --add-to-queue"
        )

    # 2. (run after 1) Run the integrations individually.
    # TODO: should be slurm jobs.
    for integration in INTEGRATIONS_LIST:
        print(f"python app.py --integration {integration} --run-integrations")


if __name__ == "__main__":
    main()
