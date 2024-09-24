"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""

from datetime import datetime, timedelta, timezone
import json
from typing import Literal, Optional

from lib.constants import timestamp_format
from lib.helper import generate_current_datetime_str
from lib.helper import create_batches, track_performance
from lib.log.logger import get_logger
from ml_tooling.llm.inference import run_batch_queries
from services.ml_inference.helper import get_posts_to_classify, insert_labeling_session  # noqa
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel,
    LLMSociopoliticalLabelsModel,
    SociopoliticalLabelsModel,
)
from services.ml_inference.sociopolitical.export_data import (
    export_results,
    write_post_to_cache,
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

logger = get_logger(__name__)
LLM_MODEL_NAME = "GPT-4o mini"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MINIBATCH_SIZE = 10
# NOTE: will need to change as we make the sociopolitical lambda more efficient.
# max_num_posts = 1000  # given our batching, we can handle ~500 posts/minute.
max_num_posts = 5000  # given our batching, we can handle ~500 posts/minute.


def generate_prompt(posts: list[FilteredPreprocessedPostModel]) -> str:
    """Generates a prompt for the LLM."""
    enumerated_texts = ""
    for i, post in enumerate(posts):
        post_text = post.text.strip()
        enumerated_texts += f"{i+1}. {post_text}\n"
    prompt = f"""
You are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "left", "right", or 'unclear'. \
If the text is not sociopolitical, return "unclear". Base your response on US politics.\

Think through your response step by step.

Do NOT include any explanation. Only return the JSON output.

TEXT:
```
{enumerated_texts}
```
"""
    return prompt


def parse_llm_result(
    json_result: str, expected_number_of_posts: int
) -> list[LLMSociopoliticalLabelModel]:  # noqa
    json_result: LLMSociopoliticalLabelsModel = LLMSociopoliticalLabelsModel(
        **json.loads(json_result)
    )
    label_models: list[LLMSociopoliticalLabelModel] = json_result.labels
    if len(label_models) != expected_number_of_posts:
        raise ValueError(
            f"Number of results ({len(label_models)}) does not match number of posts ({expected_number_of_posts})."
        )
    return label_models


def process_sociopolitical_batch(
    posts: list[FilteredPreprocessedPostModel],
) -> list[LLMSociopoliticalLabelModel]:
    """Takes batch and runs the LLM for it."""
    minibatches = [
        posts[i : i + DEFAULT_MINIBATCH_SIZE]
        for i in range(0, len(posts), DEFAULT_MINIBATCH_SIZE)
    ]
    prompts = [generate_prompt(batch) for batch in minibatches]
    json_results: list[str] = run_batch_queries(
        prompts, role="user", model_name=LLM_MODEL_NAME
    )
    all_results: list[LLMSociopoliticalLabelModel] = []
    for json_result, minibatch in zip(json_results, minibatches):
        try:
            results: list[LLMSociopoliticalLabelModel] = parse_llm_result(
                json_result=json_result, expected_number_of_posts=len(minibatch)
            )
            all_results.extend(results)
        except ValueError as e:
            # NOTE: taking this approach for now to avoid errors in the batch and
            # to just return empty results. We won't write any posts that are
            # misclassified; we'll revisit how to implement this better later.
            logger.error(f"Error parsing LLM result: {e}")
    return all_results


def export_validated_llm_output(
    posts: list[FilteredPreprocessedPostModel],
    results: list[LLMSociopoliticalLabelModel],
    source_feed: Literal["firehose", "most_liked"],
) -> SociopoliticalLabelsModel:
    """Write the validated LLM output to the database."""
    label_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H:%M:%S")
    output_models: list[SociopoliticalLabelsModel] = [
        SociopoliticalLabelsModel(
            uri=post.uri,
            text=post.text,
            llm_model_name=LLM_MODEL_NAME,
            was_successfully_labeled=True,
            label_timestamp=label_timestamp,
            is_sociopolitical=result.is_sociopolitical,
            political_ideology_label=result.political_ideology_label,
        )
        for post, result in zip(posts, results)
    ]
    for output_model in output_models:
        write_post_to_cache(output_model, source_feed=source_feed)
    return output_models


def process_llm_batch(
    post_batch: list[FilteredPreprocessedPostModel],
    source_feed: Literal["firehose", "most_liked"],
) -> dict:
    """Process a batch of posts and prompt using the LLM.

    Retries up to `max_retries` times if the output is invalid. If the output
    is valid, then attempts to write the result.

    Returns a dict based on if the processing failed. If failed, returns the
    post batch and prompt to retry later.
    """
    inserted_results: list[LLMSociopoliticalLabelModel] = []
    results: list[LLMSociopoliticalLabelModel] = process_sociopolitical_batch(
        posts=post_batch
    )  # noqa
    if len(results) > 0:
        output_models: list[SociopoliticalLabelsModel] = export_validated_llm_output(  # noqa
            posts=post_batch, results=results, source_feed=source_feed
        )
        inserted_results.extend(output_models)
        return {"succeeded": True, "response": inserted_results}
    else:
        return {"succeeded": False, "response": []}


@track_performance
def batch_classify_posts(
    posts: list[FilteredPreprocessedPostModel],
    source_feed: Literal["firehose", "most_liked"],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> list[SociopoliticalLabelsModel]:
    """Classify posts in batches."""
    post_batches: list[list[FilteredPreprocessedPostModel]] = create_batches(
        batch_list=posts, batch_size=batch_size
    )
    successful_batches: list[SociopoliticalLabelsModel] = []
    failed_batches: list[tuple] = []
    num_batches = len(post_batches)
    for i, batch in enumerate(post_batches):
        if i % 10 == 0:
            logger.info(f"Processing batch {i} of {num_batches}...")
        result_dict = process_llm_batch(post_batch=batch, source_feed=source_feed)  # noqa
        if result_dict["succeeded"]:
            successful_batches.extend(result_dict["response"])
        else:
            failed_batches.extend(result_dict["response"])
    return successful_batches


@track_performance
def run_batch_classification(
    posts: list[FilteredPreprocessedPostModel],
    source_feed: Literal["firehose", "most_liked"],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> list[SociopoliticalLabelsModel]:
    results: list[SociopoliticalLabelsModel] = batch_classify_posts(
        posts=posts,
        source_feed=source_feed,
        batch_size=batch_size,
    )
    return results


def classify_latest_posts(
    skip_inference: bool = False,
    backfill_period: Optional[str] = None,
    backfill_duration: Optional[int] = None,
):
    """Classifies the latest posts using LLM inference.

    NOTE: for now we're just using an LLM. Would be nice to eventually use a
    fine-tuned BERT model, but we'll revisit this later.
    """
    if backfill_duration is not None and backfill_period in ["days", "hours"]:
        current_time = datetime.now(timezone.utc)
        if backfill_period == "days":
            backfill_time = current_time - timedelta(days=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} days of data.")
        elif backfill_period == "hours":
            backfill_time = current_time - timedelta(hours=backfill_duration)
            logger.info(f"Backfilling {backfill_duration} hours of data.")
    else:
        backfill_time = None
    if backfill_time is not None:
        backfill_timestamp = backfill_time.strftime(timestamp_format)
        timestamp = backfill_timestamp
    else:
        timestamp = None
    posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(  # noqa
        inference_type="llm", timestamp=timestamp, max_per_source=max_num_posts
    )
    logger.info(f"Classifying {len(posts_to_classify)} posts with an LLM...")  # noqa
    if len(posts_to_classify) == 0:
        logger.warning("No posts to classify with LLM. Exiting...")
        return
    firehose_posts = [post for post in posts_to_classify if post.source == "firehose"]
    most_liked_posts = [
        post for post in posts_to_classify if post.source == "most_liked"
    ]

    if not skip_inference:
        source_to_posts_tuples = [
            ("firehose", firehose_posts),
            ("most_liked", most_liked_posts),
        ]  # noqa
        for source, posts in source_to_posts_tuples:
            print(f"For source {source}, there are {len(posts)} posts.")
            posts = posts[:max_num_posts]  # Take the first X posts
            print(f"Truncating to {max_num_posts} posts.")
            # labels stored in local storage, and then loaded
            # later. This format is done to make it more
            # robust to errors and to the script failing (though
            # tbh I could probably just return the posts directly
            # and then write to S3).
            run_batch_classification(posts=posts, source_feed=source)

    # export cached results to S3 store.
    timestamp = generate_current_datetime_str()
    results = export_results(current_timestamp=timestamp, external_stores=["s3"])
    labeling_session = {
        "inference_type": "llm",
        "inference_timestamp": timestamp,
        "total_classified_posts": results["total_classified_posts"],
        "total_classified_posts_by_source": results["total_classified_posts_by_source"],  # noqa
    }
    insert_labeling_session(labeling_session)


if __name__ == "__main__":
    classify_latest_posts()
    print("LLM classification complete.")
