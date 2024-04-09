"""Performs filtering steps."""
from lib.constants import current_datetime_str
from lib.helper import track_performance
from services.filter_raw_data.classify_bots.main import filter_posts_not_written_by_bot # noqa
from services.filter_raw_data.classify_hate_speech.main import filter_posts_have_no_hate_speech # noqa
from services.filter_raw_data.classify_language.main import filter_text_is_english # noqa
from services.filter_raw_data.classify_nsfw_content.main import filter_posts_have_no_nsfw_content # noqa
from services.filter_raw_data.classify_spam.main import filter_posts_have_no_spam
from services.filter_raw_data.database import batch_create_filtered_posts


@track_performance
def filter_posts_with_filter_func(
    posts: list[dict], filter_func: callable, label: str
) -> list[dict]:
    """Filters posts with a specific filtering function.
    
    Returns a dictionary of the following format:
    :passed_filters: set[str]: the URIs of the posts that passed the filter.
    :failed_filters: set[str]: the URIs of the posts that failed the filter.

    Example:
    >>  filter_posts_with_filter_func(posts=post, filter_func=classify_spam, label="has_spam")
    """
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


def preprocess_post(post: dict) -> dict:
    """Preprocesses a single post as necessary, before filtering."""
    # preprocessing needed for language classifier
    post["text"] = post["text"].replace("\n", " ").strip()
    # remove deprecated field
    post.pop("indexed_at")
    return post


def preprocess_posts(posts: list[dict]) -> list[dict]:
    """Preprocesses the posts as necessary, before filtering."""
    return [preprocess_post(post) for post in posts]


@track_performance
def filter_posts(posts: list[dict]) -> list[dict]:
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
    """
    # do any preprocessing for posts before filtering
    posts: list[dict] = preprocess_posts(posts)

    # we need to run the language filter first since this will filter the
    # majority of posts (60% - 80% of posts per batch).
    results_after_english_filter = filter_posts_with_filter_func(
        posts=posts, filter_func=filter_text_is_english, label="is_english"
    )
    english_post_uris = results_after_english_filter["passed_filters"]

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
        post for post in posts if post["uri"] in english_post_uris
    ]

    # for each filter, we apply the filter and add the results to the output.
    # the order of these filters doesn't particularly matter, unless we have
    # a specific reason to prefer one ordering over another.
    filter_funcs = [
        filter_posts_not_written_by_bot, filter_posts_have_no_nsfw_content,
        filter_posts_have_no_spam, filter_posts_have_no_hate_speech
    ]
    labels = ["is_not_from_possible_bot_account", "has_no_nsfw_content", "has_no_spam", "has_no_hate_speech"] # noqa

    for filter_func, label in zip(filter_funcs, labels):
        results = filter_posts_with_filter_func(
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
            post for post in posts_to_filter if post["uri"] in results["passed_filters"]
        ]

    # whatever posts are left, are the ones that have passed all filters.
    res.extend([
        {
            "uri": post["uri"],
            "passed_filters": True,
            "filtered_at": current_datetime_str,
            "filtered_by_func": None
        }
        for post in posts_to_filter
    ])

    # TODO: maybe we want to have the joined results and write the joined
    # results so we can just directly use these instead of the raw posts?
    # Unsure, but think that we really only need the IDs that passed the
    # steps, but maybe for now we can just keep all the fields so we can avoid
    # having to do filtering at each downstream service and just have a fully
    # hydrated set of filtered posts? Will go back to this later but for now
    # let's use a hydrated version of each post so that downstream services
    # can just filter on the filter status. Don't think that this causes any
    # storage issues but it does save us having to do joins or filters
    # downstream where we have to grab the IDs into memory and join/filter them
    # against the raw posts.

    # join `res` and `posts`, based on uri
    join_key = "uri"
    res_dict = {post[join_key]: post for post in res}
    posts_dict = {post[join_key]: post for post in posts}
    joined_results = [
        {**posts_dict[uri], **res_dict[uri]}
        for uri in res_dict.keys()
    ]

    return joined_results


def save_filtered_posts_to_db(filtered_posts: list[dict]) -> None:
    """Saves the filtered posts to the database.

    We save all posts, whether they passed the filters or not, to the database,
    so that we can track which posts have been filtered.
    """
    batch_create_filtered_posts(filtered_posts)
