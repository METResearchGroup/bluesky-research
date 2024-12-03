"""Gets the consolidated and enriched posts that ended up in the feeds."""

from lib.db.manage_local_data import load_data_from_local_storage

analytics_data_path = "/projects/p32375/bluesky_research_data/analytics"

# get partition dates

# loop through each partition date

# load the data

# get the posts that were in the feeds

# export those to analytics/consolidated_posts/partition_date={partition_date}


def main():
    # get the partition dates to load posts from. This should be a few days
    # before the earliest feeds since it'll take posts from the previous days.
    # posts_partition_dates = get_partition_dates(
    #     start_date="2024-09-26", end_date="2024-12-01"
    # )
    consolidated_enriched_posts_df = load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        directory="cache",
    )
    print(f"Loaded {len(consolidated_enriched_posts_df)} consolidated enriched posts")
    breakpoint()


if __name__ == "__main__":
    main()
