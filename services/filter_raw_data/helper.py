"""Helper code for running filters on raw data."""
import pandas as pd

from lib.helper import track_function_runtime

from services.filter_raw_data.database import (
    batch_create_filtered_posts, get_previously_filtered_post_uris
)
from services.sync.stream.helper import get_all_posts_as_df


@track_function_runtime
def load_latest_raw_data_as_df() -> pd.DataFrame:
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
    # load IDs from FilteredFirehosePost table.
    previously_filtered_post_uris: set[str] = set(
        get_previously_filtered_post_uris()
    )

    # we filter the IDs in a pandas dataframe, since adding them all to the
    # WHERE clause becomes really inefficient (due to SQL parsing constraints
    # plus query string limits).
    all_raw_posts: pd.DataFrame = get_all_posts_as_df()
    latest_raw_data_df: pd.DataFrame = all_raw_posts[
        ~all_raw_posts["uri"].isin(previously_filtered_post_uris)
    ]
    return latest_raw_data_df


@track_function_runtime
def filter_raw_data_non_ml(posts: pd.DataFrame) -> list[str]:
    """Runs non-ML-powered filters on raw data.
    
    Returns a list of string URIs that passed the non-ML filters.
    """
    pass


@track_function_runtime
def filter_raw_data_ml(posts: pd.DataFrame) -> list[str]:
    """Runs ML-powered filters on raw data.
    
    Returns a list of string URIs that passed the ML filters.
    """
    pass


@track_function_runtime
def combine_filtered_data_results(
    non_ml_filter_results: list[str],
    ml_filter_results: list[str]
) -> pd.DataFrame:
    """Combines the results of the non-ML and ML filters.
    
    Takes as input the list of URIs that passed the filtering steps
    """
    filter_results = [
        (post_id, True)
        for post_id in non_ml_filter_results + ml_filter_results
    ]
    filter_results_df = pd.DataFrame(
        filter_results,
        columns=["uri", "passed_filters"]
    )
    return filter_results_df


@track_function_runtime
def create_filtered_data_results(
    latest_raw_data_df: pd.DataFrame, combined_filter_results_df: pd.DataFrame
) -> pd.DataFrame:
    """Creates an updated dataframe that has the results of all the uris and if
    they passed the filters or not.
    
    Does a join of the posts that passed the filters with the latest raw data,
    so that any uris that didn't pass the filters will be in the raw data and
    be imputed with "False".
    """
    uri_to_filter_status_df = latest_raw_data_df["uri"].merge(
        combined_filter_results_df,
        on="uri",
        how="left"
    )
    uri_to_filter_status_df["passed_filters"].fillna(False, inplace=True)
    # add timestamp of filtering
    uri_to_filter_status_df["filtered_at"] = pd.Timestamp.now()
    return uri_to_filter_status_df


@track_function_runtime
def write_filtered_data_to_db(uri_to_filter_status_df: pd.DataFrame) -> None:
    """Writes filtered data to DB.
    
    Writes in a table, `validated_posts_after_filters`, to track the status of the
    filtered data.

    Each row will have the post ID and TRUE or FALSE depending on whether
    the post passed the filters (TRUE) or not (FALSE).
    """
    uri_to_filter_status_dicts: list[dict] = (
        uri_to_filter_status_df.to_dict(orient="records")
    )
    batch_create_filtered_posts(uri_to_filter_status_dicts)


@track_function_runtime
def filter_latest_raw_data():
    """Filters the latest raw data."""
    latest_raw_data_df: pd.DataFrame = load_latest_raw_data_as_df()
    num_posts = latest_raw_data_df.shape[0]
    print(f"Loaded {num_posts} posts for filtering.")
    non_ml_filter_results: list[str] = filter_raw_data_non_ml(latest_raw_data_df)
    ml_filter_results: list[str] = filter_raw_data_ml(latest_raw_data_df)
    combined_filter_results_df: pd.DataFrame = combine_filtered_data_results(
        non_ml_filter_results, ml_filter_results
    )
    uri_to_filter_status_df = create_filtered_data_results(
        latest_raw_data_df=latest_raw_data_df,
        combined_filter_results_df=combined_filter_results_df
    )
    write_filtered_data_to_db(uri_to_filter_status_df)
    num_posts_passed_filters = uri_to_filter_status_df["passed_filters"].sum()
    print(f"Filtered data written to DB. After filtering, {num_posts_passed_filters} posts passed the filters (out of {num_posts} original posts).") # noqa
