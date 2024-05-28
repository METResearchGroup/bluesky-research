"""Performs filtering steps."""
from lib.constants import current_datetime_str
from lib.helper import track_performance
from lib.log.logger import Logger
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from services.preprocess_raw_data.classify_bots.main import filter_posts_not_written_by_bot  # noqa
from services.preprocess_raw_data.classify_hate_speech.main import filter_posts_have_no_hate_speech  # noqa
from services.preprocess_raw_data.classify_language.helper import preprocess_text_for_filtering  # noqa
from services.preprocess_raw_data.classify_language.main import filter_text_is_english  # noqa
from services.preprocess_raw_data.classify_nsfw_content.main import filter_posts_have_no_nsfw_content  # noqa
from services.preprocess_raw_data.classify_spam.main import filter_posts_have_no_spam  # noqa
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

logger = Logger(__name__)


@track_performance
def filter_posts_with_filter_func(
    posts: list[ConsolidatedPostRecordModel],
    filter_func: callable,
    label: str
) -> dict[str, set]:
    """Filters posts with a specific filtering function.

    Returns a dictionary of the following format:
    :passed_filters: set[str]: the URIs of the posts that passed the filter.
    :failed_filters: set[str]: the URIs of the posts that failed the filter.

    Example:
    >>  filter_posts_with_filter_func(posts=post, filter_func=classify_spam, label="has_spam")
    """  # noqa
    passed_filters_uris: set[str] = set()
    failed_filters_uris: set[str] = set()

    # batch classify posts and let each service manage batching accordingly.
    results: list[dict] = filter_func(posts)
    label_results = [res[label] for res in results]

    for (label, res) in zip(label_results, results):
        if label:
            passed_filters_uris.add(res["uri"])
        else:
            failed_filters_uris.add(res["uri"])

    return {
        "passed_filters": passed_filters_uris,
        "failed_filters": failed_filters_uris
    }


def preprocess_post(
    post: ConsolidatedPostRecordModel
) -> ConsolidatedPostRecordModel:
    """Preprocesses a single post as necessary, before filtering."""
    # preprocessing needed for language classifier. Specifically, removes any
    # newline chars, which the classifier doesn't like.
    post_text = post.record.text
    processed_text = preprocess_text_for_filtering(post_text)
    post.record.text = processed_text
    return post


def preprocess_posts(
    posts: list[ConsolidatedPostRecordModel]
) -> list[ConsolidatedPostRecordModel]:
    """Preprocesses the posts as necessary, before filtering."""
    res: list[ConsolidatedPostRecordModel] = [
        preprocess_post(post) for post in posts
    ]
    return res


@track_performance
def filter_posts(
    posts: list[ConsolidatedPostRecordModel]
) -> list[FilteredPreprocessedPostModel]:
    """Applies the filtering steps and returns the posts along with their
    status.

    Returns the following fields per dictionary:
    :uri: str: The URI of the post.
    :passed_filters: bool: Whether the post passed the filters or not.
    :filtered_at: datetime: The timestamp of when the post was filtered.
    :filtered_by_func: if filtered out, which function filtered it out.

    Each filtering function returns the following for a given post:
    :uri: str: The URI of the post.
    :<filter_name>: bool: Whether the post passed the filter or not.

    Example:
        - {"uri": post["uri"], "has_spam": has_spam}

    For posts, we run the filtering function and return two sets of URIs:
    :passed_filters: set[str]: the URIs of the posts that passed the filter.
    :failed_filters: set[str]: the URIs of the posts that failed the filter.

    We add the URIs of those that failed that filter to the output. We then
    pass to the next filter only the URIs that passed the previous filter.

    After all the filters are done, we add the remaining URIs to the output
    as the URIs of the posts that have passed all the filters.
    """  # noqa
    # do any preprocessing for posts before filtering
    logger.info("Starting post filtering in preprocessing pipeline.")
    posts: list[ConsolidatedPostRecordModel] = preprocess_posts(posts)
    logger.info(f"Total posts for filtering: {len(posts)}")

    # we need to run the language filter first since this will filter the
    # majority of posts (60% - 80% of posts per batch).
    results_after_english_filter: dict = filter_posts_with_filter_func(
        posts=posts, filter_func=filter_text_is_english, label="is_english"
    )
    english_post_uris: set = results_after_english_filter["passed_filters"]
    logger.info(f"After English filtering, number of posts that passed filter: {len(english_post_uris)}")  # noqa
    logger.info(f"After English filtering, number of posts that failed filter: {len(results_after_english_filter['failed_filters'])}")  # noqa

    # for the posts that have been filtered out, let's add them to our
    # output.
    res = [
        {
            "uri": uri,
            "passed_filters": False,
            "filtered_at": current_datetime_str,
            "filtered_by_func": filter_text_is_english.__name__
        }
        for uri in results_after_english_filter["failed_filters"]
    ]

    # we apply downstream filters only on English posts.
    posts_to_filter = [
        post for post in posts if post.uri in english_post_uris
    ]

    # for each filter, we apply the filter and add the results to the output.
    # the order of these filters doesn't particularly matter, unless we have
    # a specific reason to prefer one ordering over another.
    filter_funcs_with_labels: list[tuple] = [
        (filter_posts_not_written_by_bot, "is_not_from_possible_bot_account"),
        (filter_posts_have_no_nsfw_content, "has_no_nsfw_content"),
        (filter_posts_have_no_spam, "has_no_spam"),
        (filter_posts_have_no_hate_speech, "has_no_hate_speech")
    ]

    for (filter_func, label) in filter_funcs_with_labels:
        results: dict = filter_posts_with_filter_func(
            posts=posts_to_filter, filter_func=filter_func, label=label
        )
        res.extend([
            {
                "uri": uri,
                "passed_filters": False,
                "filtered_at": current_datetime_str,
                "filtered_by_func": filter_func.__name__
            }
            for uri in results["failed_filters"]
        ])
        # update the posts to filter if it has passed all the filters so far.
        posts_to_filter = [
            post
            for post in posts_to_filter
            if post.uri in results["passed_filters"]
        ]
        func_name = filter_func.__name__
        logger.info(f"Finished filtering with function: {func_name}")
        logger.info(f"Posts passing filter: {len(results['passed_filters'])}. Posts failing filter: {len(results['failed_filters'])}.")  # noqa
        logger.info(f"Posts remaining after filtering with {func_name}: {len(posts_to_filter)}")  # noqa

    # whatever posts are left, are the ones that have passed all filters.
    res.extend([
        {
            "uri": post.uri,
            "passed_filters": True,
            "filtered_at": current_datetime_str,
            "filtered_by_func": None
        }
        for post in posts_to_filter
    ])

    # we now create a hash map of the results, with URI as the key.
    uri_to_results_map = {result["uri"]: result for result in res}

    # we then work through the original list of posts and create the resulting
    # objects accordingly.
    filtered_posts: list[FilteredPreprocessedPostModel] = []
    for post in posts:
        uri = post.uri
        filtering_results = uri_to_results_map[uri]
        filtered_post_result = {
            "uri": uri,
            "cid": post.cid,
            "indexed_at": post.indexed_at,
            "author": post.author,
            "metadata": post.metadata,
            "record": post.record,
            "metrics": post.metrics,
            "passed_filters": filtering_results["passed_filters"],
            "filtered_at": filtering_results["filtered_at"],
            "filtered_by_func": filtering_results["filtered_by_func"],
            "synctimestamp": post.metadata.synctimestamp,
            "preprocessing_timestamp": current_datetime_str
        }
        filtered_post = FilteredPreprocessedPostModel(**filtered_post_result)
        filtered_posts.append(filtered_post)

    logger.info("Completed post filtering in preprocessing pipeline.")
    breakpoint()
    return filtered_posts
