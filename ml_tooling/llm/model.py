"""Model for the sociopolitical LLM service."""

from datetime import datetime, timezone
import json
from typing import Optional

from lib.constants import timestamp_format
from lib.helper import create_batches, track_performance
from lib.log.logger import get_logger
from ml_tooling.llm.inference import run_batch_queries
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel,
    LLMSociopoliticalLabelsModel,
    SociopoliticalLabelsModel,
)
from services.ml_inference.sociopolitical.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)


logger = get_logger(__name__)
LLM_MODEL_NAME = "GPT-4o mini"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MINIBATCH_SIZE = 10
# max_num_posts = 15_000  # given our batching, we can handle ~500 posts/minute.
max_num_posts = 40_000  # should run in ~40 minutes with current runtime.


def generate_prompt(posts: list[dict]) -> str:
    """Generates a prompt for the LLM."""
    enumerated_texts = ""
    for i, post in enumerate(posts):
        post_text = post["text"].strip()
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


# TODO: check the actual model inference code at some point.
def process_sociopolitical_batch(posts: list[dict]) -> list[dict]:
    """Takes batch and runs the LLM for it.

    Splits the batch of posts into minibatches. I've found a pragmatic limit
    of 10 posts per prompt, so this splits the batch (e.g., batch of 100 posts)
    into minibatches of size 10.

    Returns the list of results from the LLM. If a minibatch wasn't labeled
    successfully, returns a list of dicts, which will either be populated with
    the label or will be an empty dict.
    """
    minibatches: list[list[dict]] = [
        posts[i : i + DEFAULT_MINIBATCH_SIZE]
        for i in range(0, len(posts), DEFAULT_MINIBATCH_SIZE)
    ]
    prompts: list[str] = [generate_prompt(batch) for batch in minibatches]
    json_results: list[str] = run_batch_queries(
        prompts, role="user", model_name=LLM_MODEL_NAME
    )
    results: list[dict] = []
    for json_result, minibatch in zip(json_results, minibatches):
        try:
            parsed_llm_results: list[LLMSociopoliticalLabelModel] = parse_llm_result(
                json_result=json_result, expected_number_of_posts=len(minibatch)
            )
            results.extend([result.model_dump() for result in parsed_llm_results])
        except ValueError as e:
            logger.error(f"Error parsing LLM result: {e}")
            results.extend([{}] * len(minibatch))
    return results


def create_labels(posts: list[dict], responses: list[dict]) -> list[dict]:
    """Create label models from posts and responses.

    If there are no posts, return an empty list.
    """
    if not posts:
        return []
    label_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
    res = []
    for post, response_obj in zip(posts, responses):
        label = SociopoliticalLabelsModel(
            uri=post["uri"],
            text=post["text"],
            llm_model_name=LLM_MODEL_NAME,
            was_successfully_labeled=True if response_obj else False,
            label_timestamp=label_timestamp,
            is_sociopolitical=response_obj.get("is_sociopolitical", None),
            political_ideology_label=response_obj.get("political_ideology_label", None),
        )
        res.append(label.model_dump())
    return res


@track_performance
def batch_classify_posts(
    posts: list[dict],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> dict:
    """Classify posts in batches."""
    batches: list[list[dict]] = create_batches(batch_list=posts, batch_size=batch_size)
    total_batches = len(batches)
    total_posts_successfully_labeled = 0
    total_posts_failed_to_label = 0
    for i, batch in enumerate(batches):
        if i % 10 == 0:
            logger.info(f"Processing batch {i}/{total_batches}")
        responses: list[dict] = process_sociopolitical_batch(post_batch=batch)
        labels: list[dict] = create_labels(posts=batch, responses=responses)

        successful_labels: list[dict] = []
        failed_labels: list[dict] = []

        total_failed_labels = 0
        total_successful_labels = 0

        # we add the batch ID to the label model. This way, we can know which
        # batches to delete from the input queue. Any batch IDs that appear in
        # the successfully labeled set will be deleted from the input queue (
        # since any failed labels will be re-inserted into the input queue).
        for post, label in zip(batch, labels):
            post_batch_id = post["batch_id"]
            label["batch_id"] = post_batch_id
            if label["was_successfully_labeled"]:
                successful_labels.append(label)
                total_successful_labels += 1
            else:
                failed_labels.append(label)
                total_failed_labels += 1

        if total_failed_labels > 0:
            logger.error(
                f"Failed to label {total_failed_labels} posts. Re-inserting these into queue."
            )
            return_failed_labels_to_input_queue(
                failed_labels=failed_labels,
                batch_size=batch_size,
            )
            total_posts_failed_to_label += total_failed_labels
        else:
            logger.info(f"Successfully labeled {total_successful_labels} posts.")
            write_posts_to_cache(
                posts=successful_labels,
                batch_size=batch_size,
            )
            total_posts_successfully_labeled += total_successful_labels
        del successful_labels
        del failed_labels
    return {
        "total_batches": total_batches,
        "total_posts_successfully_labeled": total_posts_successfully_labeled,
        "total_posts_failed_to_label": total_posts_failed_to_label,
    }


@track_performance
def run_batch_classification(
    posts: list[dict],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> dict:
    metadata = batch_classify_posts(posts=posts, batch_size=batch_size)
    return metadata
