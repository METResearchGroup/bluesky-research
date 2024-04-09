"""Helper code for running filters on raw data."""
from typing import Optional

import pandas as pd

from lib.helper import track_performance
from services.filter_raw_data.database import get_previously_filtered_post_uris
from services.filter_raw_data.filters import filter_posts, save_filtered_posts_to_db # noqa
from services.sync.stream.helper import get_posts_as_df

DEFAULT_BATCH_SIZE = 100000


@track_performance
def load_latest_raw_posts(
    max_num_raw_posts: Optional[int] = None
) -> list[dict]:
    """Loads raw data from the firehose DB.

    Excludes previously preprocessed data.

    Ideally we would be able to do this filter via left join so we don't have
    to load in all this data into memory, but this is a limitation if we have
    data in different databases. Might revisit in the future if scalability
    is a problem.

    For now, we want to keep the databases separate so that we can scale
    firehost posts separately from the rest of the data. Plus, we will
    subscribe to the firehose data in the future, so we want to keep it
    capable of doing writes and not be impeded.

    NOTE: a better solution in the future might be to use the latest filter
    timestamp as a filter, and we can say something like "filter only posts
    published after the last filter timestamp" or something like that.
    """
    print("Loading latest raw data.")
    # load IDs from FilteredFirehosePost table.
    previously_filtered_post_uris: set[str] = set(
        get_previously_filtered_post_uris()
    )

    # we filter the IDs in a pandas dataframe, since adding them all to the
    # WHERE clause becomes really inefficient (due to SQL parsing constraints
    # plus query string limits).
    all_raw_posts: pd.DataFrame = get_posts_as_df(k=max_num_raw_posts)
    # NOTE: faster to df -> filter -> dicts than dicts -> filter?
    latest_raw_data_df: pd.DataFrame = all_raw_posts[
        ~all_raw_posts["uri"].isin(previously_filtered_post_uris)
    ]
    latest_raw_data_dicts = latest_raw_data_df.to_dict(orient="records")
    print(f"Finished loading {len(latest_raw_data_dicts)} raw posts for filtering.") # noqa
    return latest_raw_data_dicts


@track_performance
def filter_latest_raw_data(
    max_num_raw_posts: Optional[int] = DEFAULT_BATCH_SIZE
):
    """Filters the latest raw data.

    Loads the latest posts, filters them, and writes the filtered data to the
    database. Writes all posts and their filtered status, so we can track
    which posts passed the filters and which ones didn't and so we don't
    duplicate filtering in the future.
    """
    latest_raw_posts: list[dict] = load_latest_raw_posts(
        max_num_raw_posts=max_num_raw_posts
    )
    num_posts: int = len(latest_raw_posts)
    print(f"Loaded {num_posts} posts for filtering.")
    filtered_posts: list[dict] = filter_posts(posts=latest_raw_posts)
    save_filtered_posts_to_db(filtered_posts)
    num_posts_passed_filters = sum(
        post["passed_filters"] for post in filtered_posts
    )
    print(f"Filtered data written to DB. After filtering, {num_posts_passed_filters} posts passed the filters (out of {num_posts} original posts).") # noqa
