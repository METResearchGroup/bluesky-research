"""Model for the sociopolitical LLM service."""

from datetime import datetime, timezone
import json
import time
from typing import Optional

from lib.constants import timestamp_format
from lib.helper import create_batches, track_performance
from lib.log.logger import get_logger
from ml_tooling.llm.llm_service import get_llm_service
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel,
    LLMSociopoliticalLabelsModel,
    SociopoliticalLabelsModel,
)
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)


logger = get_logger(__name__)
LLM_MODEL_NAME = "gpt-4o-mini"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MINIBATCH_SIZE = 10
# max_num_posts = 15_000  # given our batching, we can handle ~500 posts/minute.
max_num_posts = 40_000  # should run in ~40 minutes with current runtime.


def generate_prompt(posts: list[dict]) -> str:
    """Generates a structured prompt for LLM-based sociopolitical classification.

    Args:
        posts (list[dict]): A list of dictionaries, each representing a post. Each dictionary must contain a "text" key with the post's content.

    Returns:
        str: A formatted prompt string that includes numbered texts of posts along with instructions for classification.

    Behavior:
        1. Iterates over the list of posts, strips whitespace from each post's "text" field, and enumerates them starting at 1.
        2. Joins all post texts into a single string and interpolates them into a multi-line template that instructs the LLM on how to classify posts as "sociopolitical" or "not sociopolitical".

    [Suggestions]:
        - Consider validating the presence of the "text" key in each post.

    [Clarifications]:
        - Is the output prompt intended for direct submission to an LLM?
    """
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

Think through your response step by step.\

Do NOT include any explanation. Only return the JSON output. You must
return an output for each of the enumerated posts, even if the output
would be the same for all posts. For example, if 10 posts are provided,
I expect 10 outputs in the JSON.

TEXT:
```
{enumerated_texts}
``` 
"""
    return prompt


def parse_llm_result(
    json_result: str, expected_number_of_posts: int
) -> list[LLMSociopoliticalLabelModel]:
    """Parses the JSON result obtained from an LLM query and returns a list of label models.

    Args:
        json_result (str): A JSON-formatted string containing LLM classification results.
        expected_number_of_posts (int): The expected count of classification outputs (number of posts processed).

    Returns:
        list[LLMSociopoliticalLabelModel]: A list of label model objects parsed from the JSON string.

    Behavior:
        - Deserializes the JSON string into a LLMSociopoliticalLabelsModel object.
        - Extracts the 'labels' attribute, ensuring its length matches expected_number_of_posts.
        - Raises a ValueError if the count does not match.

    [Suggestions]:
        - Consider adding more robust error handling if the JSON schema changes.

    [Clarifications]:
        - Should additional fields be verified in the response?
    """
    json_result_obj: LLMSociopoliticalLabelsModel = LLMSociopoliticalLabelsModel(
        **json.loads(json_result)
    )
    label_models: list[LLMSociopoliticalLabelModel] = json_result_obj.labels
    if len(label_models) != expected_number_of_posts:
        raise ValueError(
            f"Number of results ({len(label_models)}) does not match number of posts ({expected_number_of_posts})."
        )
    return label_models


def process_sociopolitical_batch(posts: list[dict]) -> list[dict | None]:
    """Processes a batch of posts through LLM classification by splitting them into mini-batches and aggregating the results.

    Args:
        posts (list[dict]): A list of dictionaries, each representing a post, where each post must include a "text" key.

    Returns:
        list[dict | None]: A list of result dictionaries from the LLM classification for each post; if a mini-batch fails, corresponding entries are None.

    Behavior:
        1. Splits the posts into mini-batches of size defined by DEFAULT_MINIBATCH_SIZE.
        2. Generates a prompt for each mini-batch using generate_prompt.
        3. Sends the prompts to the LLM via structured_batch_completion.
        4. For each mini-batch, validates that the number of labels matches the number of posts.
        5. Logs errors and replaces results with None if validation fails.
        6. Combines all mini-batch results into a single list and returns it.
    """
    if not posts:
        return []

    minibatches: list[list[dict]] = [
        posts[i : i + DEFAULT_MINIBATCH_SIZE]
        for i in range(0, len(posts), DEFAULT_MINIBATCH_SIZE)
    ]
    prompts: list[str] = [generate_prompt(batch) for batch in minibatches]
    structured_results: list[LLMSociopoliticalLabelsModel] = (
        get_llm_service().structured_batch_completion(prompts=prompts, response_model=LLMSociopoliticalLabelsModel, model=LLM_MODEL_NAME, role="user")
    )
    if len(structured_results) != len(minibatches):
        logger.warning(
            f"Number of results ({len(structured_results)}) does not match number of mini-batches ({len(minibatches)}). Need to retry."
        )
        return [None] * len(posts)
    results: list[dict | None] = []
    for structured_result, minibatch in zip(structured_results, minibatches):
        try:
            label_models: list[LLMSociopoliticalLabelModel] = structured_result.labels
            if len(label_models) != len(minibatch):
                raise ValueError(
                    f"Number of labels ({len(label_models)}) does not match number of posts ({len(minibatch)})."
                )
            results.extend([result.model_dump() for result in label_models])
        except ValueError as e:
            logger.error(f"Error validating LLM result: {e}")
            results.extend([None for _ in range(len(minibatch))])
    return results


def process_sociopolitical_batch_with_retries(
    posts: list[dict],
    max_retries: int = 4,
    initial_delay: float = 1.0,
) -> list[dict | None]:
    """Processes a batch of posts through LLM classification with retry logic.

    Args:
        posts (list[dict]): A list of dictionaries, each representing a post, where each post must include a "text" key.
        max_retries (int): The maximum number of retry attempts. If negative, treated as 0.
        initial_delay (float): The initial delay in seconds before the first retry. If negative, absolute value is used.

    Returns:
        list[dict | None]: A list of result dictionaries from the LLM classification for each post.
            Successful results contain classification data, failed results are None.

    Raises:
        KeyError: If any post in the input list is missing the required "text" field.
    """
    if not posts:
        return []

    # Input validation
    for post in posts:
        if "text" not in post:
            raise KeyError("All posts must contain a 'text' field")

    # Normalize parameters
    max_retries = max(0, max_retries)
    initial_delay = abs(initial_delay)

    # Make initial attempt
    final_results = process_sociopolitical_batch(posts)

    # Setup for retries if needed
    posts_to_retry = []
    retry_indices = []
    for idx, result in enumerate(final_results):
        if result is None:
            posts_to_retry.append(posts[idx])
            retry_indices.append(idx)

    retries = 0
    while posts_to_retry and retries < max_retries:
        # Sleep with exponential backoff before each retry
        time.sleep(initial_delay * (2**retries))
        retries += 1

        results = process_sociopolitical_batch(posts_to_retry)

        # Process results and track which need retry
        new_posts_to_retry = []
        new_retry_indices = []

        for idx, (result, original_idx) in enumerate(zip(results, retry_indices)):
            if result is None:
                new_posts_to_retry.append(posts_to_retry[idx])
                new_retry_indices.append(original_idx)
            else:
                final_results[original_idx] = result

        if new_posts_to_retry:
            logger.warning(
                f"{len(new_posts_to_retry)} posts failed to be labeled. Retrying..."
            )
            posts_to_retry = new_posts_to_retry
            retry_indices = new_retry_indices
        else:
            break

    return final_results


def create_labels(posts: list[dict], responses: list[dict | None]) -> list[dict]:
    """Creates label models by combining post data with corresponding LLM responses.

    Args:
        posts (list[dict]): A list of post dictionaries. Each dictionary should contain keys like "uri", "text", and "preprocessing_timestamp".
        responses (list[dict | None]): A list of response dictionaries produced by the LLM for each corresponding post, or None for failed responses.

    Returns:
        list[dict]: A list of dictionaries representing the labeled post information, which includes:
            - uri: The unique identifier of the post.
            - text: The original text content of the post.
            - llm_model_name: The LLM model used for classification.
            - was_successfully_labeled: A boolean flag indicating success of labeling.
            - preprocessing_timestamp: The timestamp of when preprocessing occurred (UTC, formatted per timestamp_format).
            - label_timestamp: The timestamp of when labeling occurred (UTC, formatted per timestamp_format).
            - is_sociopolitical: The classification result for sociopolitical content.
            - political_ideology_label: The classification of political lean, if available.

    Behavior:
        1. Retrieves the current UTC timestamp formatted by timestamp_format.
        2. Iterates through posts and their corresponding responses to create a standardized label model.
        3. Returns an aggregated list of these labeled dictionaries.
    """
    if not posts:
        return []

    if len(responses) != len(posts):
        logger.warning(
            f"Number of responses ({len(responses)}) does not match number of posts ({len(posts)}). Likely means that some posts failed to be labeled. Re-inserting all posts into queue..."
        )
        responses = [None] * len(posts)

    label_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
    res = []
    for post, response_obj in zip(posts, responses):
        label = SociopoliticalLabelsModel(
            uri=post["uri"],
            text=post["text"],
            preprocessing_timestamp=post["preprocessing_timestamp"],
            llm_model_name=LLM_MODEL_NAME,
            was_successfully_labeled=True if response_obj else False,
            label_timestamp=label_timestamp,
            is_sociopolitical=response_obj.get("is_sociopolitical", None)
            if response_obj
            else None,
            political_ideology_label=response_obj.get("political_ideology_label", None)
            if response_obj
            else None,
        )
        res.append(label.model_dump())
    return res


@track_performance
def batch_classify_posts(
    posts: list[dict],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> dict:
    """Classifies posts in batches using LLM inference and aggregates classification results.

    Args:
        posts (list[dict]): A list of post dictionaries representing the posts to classify.
        batch_size (Optional[int]): The number of posts to process per batch. Defaults to DEFAULT_BATCH_SIZE.

    Returns:
        dict: A summary dictionary of the batch classification process containing:
            - total_batches (int): The number of batches processed.
            - total_posts_successfully_labeled (int): Total number of posts successfully labeled.
            - total_posts_failed_to_label (int): Total number of posts that failed to be labeled.

    Behavior:
        1. Splits posts into batches of up to batch_size posts using create_batches.
        2. For each batch:
            - Logs progress every 10 batches.
            - Processes the batch with process_sociopolitical_batch to obtain raw responses.
            - Uses create_labels to convert responses into structured labels.
            - Separates successful labels from failed labels based on the was_successfully_labeled flag.
            - If any labels in a batch fail, logs the failure and re-inserts them into the processing queue.
            - Otherwise, caches the successful labels.
        3. Aggregates and returns a summary of the total batches processed and label outcomes.
    """
    batches: list[list[dict]] = create_batches(batch_list=posts, batch_size=batch_size)
    total_batches = len(batches)
    total_posts_successfully_labeled = 0
    total_posts_failed_to_label = 0
    for i, batch in enumerate(batches):
        if i % 10 == 0:
            logger.info(f"Processing batch {i}/{total_batches}")
        responses: list[dict | None] = process_sociopolitical_batch_with_retries(
            posts=batch
        )
        labels: list[dict] = create_labels(posts=batch, responses=responses)

        successful_labels: list[dict] = []
        failed_labels: list[dict] = []

        total_failed_labels = 0
        total_successful_labels = 0

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
                inference_type="sociopolitical",
                failed_label_models=failed_labels,
                batch_size=batch_size,
            )
            total_posts_failed_to_label += total_failed_labels
        else:
            logger.info(f"Successfully labeled {total_successful_labels} posts.")
            write_posts_to_cache(
                inference_type="sociopolitical",
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
    """Executes batch classification of posts using the LLM and returns metadata.

    Args:
        posts (list[dict]): A list of post dictionaries to classify.
        batch_size (Optional[int]): The number of posts per batch. Defaults to DEFAULT_BATCH_SIZE.

    Returns:
        dict: A metadata dictionary as returned by batch_classify_posts, summarizing classification outcomes.

    Behavior:
        - Invokes batch_classify_posts with the provided posts and batch_size.
        - Returns the resulting metadata containing counts of batches processed, successful, and failed classifications.
    """

    metadata = batch_classify_posts(posts=posts, batch_size=batch_size)
    return metadata
