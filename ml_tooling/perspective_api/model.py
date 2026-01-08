"""Model for the classify_perspective_api service."""

import asyncio
import gc
from datetime import datetime, timezone
import json
from typing import Optional, Literal

from googleapiclient import discovery
from googleapiclient.errors import HttpError

from lib.helper import create_batches, logger, track_performance  # noqa
from lib.load_env_vars import EnvVarsContainer
from services.ml_inference.models import PerspectiveApiLabelsModel
from services.ml_inference.export_data import (
    return_failed_labels_to_input_queue,
    write_posts_to_cache,
)

# current max of 100 QPS for the Perspective API. Also put a wait time of 1.05
# seconds to make it more unlikely that more than 2 batches go off in 1 second
# due to network latency.
DEFAULT_BATCH_SIZE = 90
DEFAULT_DELAY_SECONDS = 1.05  # enough to avoid some overlapping.


def get_google_client():
    return discovery.build(
        "commentanalyzer",
        "v1alpha1",
        developerKey=EnvVarsContainer.get_env_var("GOOGLE_API_KEY"),
        discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",  # noqa
        static_discovery=False,
    )


attribute_to_labels_map = {
    # production-ready attributes
    "TOXICITY": {"prob": "prob_toxic", "label": "label_toxic"},
    "SEVERE_TOXICITY": {"prob": "prob_severe_toxic", "label": "label_severe_toxic"},
    "IDENTITY_ATTACK": {
        "prob": "prob_identity_attack",
        "label": "label_identity_attack",
    },
    "INSULT": {"prob": "prob_insult", "label": "label_insult"},
    "PROFANITY": {"prob": "prob_profanity", "label": "label_profanity"},
    "THREAT": {"prob": "prob_threat", "label": "label_threat"},
    # constructive attributes, from Perspective API
    "AFFINITY_EXPERIMENTAL": {"prob": "prob_affinity", "label": "label_affinity"},
    "COMPASSION_EXPERIMENTAL": {"prob": "prob_compassion", "label": "label_compassion"},
    # According to the Perspective API team, the constructiveness endpoint has
    # been deprecated in favor of the reasoning endpoint. I'll still keep the
    # same naming convention of "constructiveness", but irl it's actually
    # the "reasoning" endpoint.
    # "CONSTRUCTIVE_EXPERIMENTAL": {
    #     "prob": "prob_constructive",
    #     "label": "label_constructive",
    # },
    "CURIOSITY_EXPERIMENTAL": {"prob": "prob_curiosity", "label": "label_curiosity"},
    "NUANCE_EXPERIMENTAL": {"prob": "prob_nuance", "label": "label_nuance"},
    "PERSONAL_STORY_EXPERIMENTAL": {
        "prob": "prob_personal_story",
        "label": "label_personal_story",
    },
    "REASONING_EXPERIMENTAL": {"prob": "prob_reasoning", "label": "label_reasoning"},
    "RESPECT_EXPERIMENTAL": {"prob": "prob_respect", "label": "label_respect"},
    # persuasion attributes
    "ALIENATION_EXPERIMENTAL": {"prob": "prob_alienation", "label": "label_alienation"},
    "FEARMONGERING_EXPERIMENTAL": {
        "prob": "prob_fearmongering",
        "label": "label_fearmongering",
    },
    "GENERALIZATION_EXPERIMENTAL": {
        "prob": "prob_generalization",
        "label": "label_generalization",
    },
    "MORAL_OUTRAGE_EXPERIMENTAL": {
        "prob": "prob_moral_outrage",
        "label": "label_moral_outrage",
    },
    "SCAPEGOATING_EXPERIMENTAL": {
        "prob": "prob_scapegoating",
        "label": "label_scapegoating",
    },
    # moderation attributes
    "SEXUALLY_EXPLICIT": {
        "prob": "prob_sexually_explicit",
        "label": "label_sexually_explicit",
    },
    "FLIRTATION": {"prob": "prob_flirtation", "label": "label_flirtation"},
    "SPAM": {"prob": "prob_spam", "label": "label_spam"},
}


default_requested_attribute_keys = list(attribute_to_labels_map.keys())
default_requested_attributes = {
    attribute: {} for attribute in default_requested_attribute_keys
}


def request_comment_analyzer(
    text: str, requested_attributes: dict = default_requested_attributes
) -> str:
    """Sends request to commentanalyzer endpoint.

    Docs at https://developers.perspectiveapi.com/s/docs-sample-requests?language=en_US

    Example request:

    analyze_request = {
    'comment': { 'text': 'friendly greetings from python' },
    'requestedAttributes': {'TOXICITY': {}}
    }

    response = client.comments().analyze(body=analyze_request).execute()
    print(json.dumps(response, indent=2))
    """  # noqa
    if not requested_attributes:
        requested_attributes = default_requested_attributes
    analyze_request = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": requested_attributes,
    }
    logger.info(
        f"Sending request to commentanalyzer endpoint with request={analyze_request}...",  # noqa
    )
    try:
        google_client = get_google_client()
        response = google_client.comments().analyze(body=analyze_request).execute()  # noqa
    except HttpError as e:
        logger.error(f"Error sending request to commentanalyzer: {e}")
        response = {"error": str(e)}
    return json.dumps(response)


def classify_text_toxicity(text: str) -> dict:
    """Classify text toxicity."""
    response = request_comment_analyzer(
        text=text, requested_attributes={"TOXICITY": {}}
    )
    response_obj = json.loads(response)
    toxicity_prob_score = response_obj["attributeScores"]["TOXICITY"]["summaryScore"][
        "value"
    ]

    return {
        "prob_toxic": toxicity_prob_score,
        "label_toxic": 0 if toxicity_prob_score < 0.5 else 1,
    }


def process_response(response_str: str) -> dict:
    response_obj = json.loads(response_str)
    if "error" in response_obj:
        return {"error": response_obj["error"]}
    classification_probs_and_labels = {}
    for attribute, labels in attribute_to_labels_map.items():
        if attribute in response_obj["attributeScores"]:
            prob_score = (
                response_obj["attributeScores"][attribute]["summaryScore"]["value"]  # noqa
            )
            classification_probs_and_labels[labels["prob"]] = prob_score
            classification_probs_and_labels[labels["label"]] = (
                0 if prob_score < 0.5 else 1
            )  # noqa
    # constructiveness == reasoning now, presumably, according to
    # the Perspective API team.
    classification_probs_and_labels["prob_constructive"] = (
        classification_probs_and_labels["prob_reasoning"]
    )
    classification_probs_and_labels["label_constructive"] = (
        classification_probs_and_labels["label_reasoning"]
    )
    return classification_probs_and_labels


def classify(
    text: str, attributes: Optional[dict] = default_requested_attributes
) -> dict:
    """Classify text using all the attributes from the Google Perspectives API."""  # noqa
    response: str = request_comment_analyzer(text=text, requested_attributes=attributes)
    return process_response(response)


def create_perspective_request(text: str) -> dict:
    """Creates a properly formatted request payload for the Perspective API.

    This function constructs a request object that conforms to the Perspective API's
    expected format, using the default set of attributes to analyze.

    Args:
        text (str): The text content to be analyzed by the Perspective API.

    Returns:
        dict: A request payload containing:
            - comment (dict): Contains the text to analyze
            - languages (list[str]): Set to ["en"] for English
            - requestedAttributes (dict): Map of all default attributes to analyze,
              where each attribute has an empty config dict. Attributes are defined
              in attribute_to_labels_map and include:
              - Production attributes: TOXICITY, SEVERE_TOXICITY, etc.
              - Experimental attributes: AFFINITY, COMPASSION, etc.
              - Persuasion attributes: ALIENATION, FEARMONGERING, etc.
              - Moderation attributes: SEXUALLY_EXPLICIT, FLIRTATION, SPAM

    Control Flow:
        1. Creates request dict with required structure
        2. Uses default_requested_attributes for attribute configuration
        3. Returns formatted request

    Note:
        - Always uses English language setting
        - Uses all available attributes defined in attribute_to_labels_map
        - Attribute configs are empty dicts, using API defaults
    """
    return {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": default_requested_attributes,
    }


async def process_perspective_batch(requests: list[dict]) -> list[dict]:
    """Processes a batch of Perspective API requests in parallel using batch execution.

    This function uses the Google API Client's batch functionality to send multiple
    requests in parallel. It manages responses through a closure-based callback system
    where each response is processed and added to a shared responses list.

    Args:
        requests (list[dict]): List of Perspective API request payloads. Each request
            must be a dict containing:
            - comment (dict): Contains 'text' key with content to analyze
            - languages (list[str]): List of language codes (e.g., ["en"])
            - requestedAttributes (dict): Map of attributes to analyze with their configs

    Returns:
        list[dict]: List of processed responses, one per request, in order. Each response
            is either:
            - A dict containing probability scores and binary labels for each requested
              attribute, including special handling for 'constructive'/'reasoning'
            - None if the request failed or returned an error

    Control Flow:
        1. If no requests, returns empty list
        2. Creates new Google API client and batch request
        3. Defines callback function that:
            a. Handles exceptions by adding None to responses
            b. Handles API errors by adding None to responses
            c. For successful responses:
                - Extracts probability scores from attributeScores
                - Converts probabilities to binary labels (threshold 0.5)
                - Maps 'reasoning' scores to 'constructive' fields
                - Adds processed results to responses list
        4. Adds each request to batch with callback
        5. Executes batch request
        6. Returns collected responses

    Note:
        - Uses closure to maintain response order through callback system
        - Automatically handles failed requests by inserting None
        - Maintains special mapping between 'reasoning' and 'constructive' attributes
        - Response order matches request order
    """
    if not requests:
        return []

    google_client = get_google_client()
    batch = google_client.new_batch_http_request()
    responses = []

    def callback(request_id, response, exception):
        if exception is not None:
            print(f"Request {request_id} failed: {exception}")
            responses.append(None)
        else:
            response_str = json.dumps(response)
            response_obj = json.loads(response_str)
            if "error" in response_obj:
                print(f"Request {request_id} failed: {response_obj['error']}")
                responses.append(None)
            else:
                classification_probs_and_labels = {}
                for attribute, labels in attribute_to_labels_map.items():
                    if attribute in response_obj["attributeScores"]:
                        prob_score = (
                            response_obj["attributeScores"][attribute]["summaryScore"][
                                "value"
                            ]  # noqa
                        )
                        classification_probs_and_labels[labels["prob"]] = prob_score  # noqa
                        classification_probs_and_labels[labels["label"]] = (
                            0 if prob_score < 0.5 else 1
                        )  # noqa
                # constructiveness == reasoning now, presumably, according to
                # the Perspective API team.
                classification_probs_and_labels["prob_constructive"] = (
                    classification_probs_and_labels["prob_reasoning"]
                )
                classification_probs_and_labels["label_constructive"] = (
                    classification_probs_and_labels["label_reasoning"]
                )
                responses.append(classification_probs_and_labels)

    for _, request in enumerate(requests):
        batch.add(google_client.comments().analyze(body=request), callback=callback)

    batch.execute()
    return responses


async def process_perspective_batch_with_retries(
    requests: list[dict],
    max_retries: int = 4,
    initial_delay: float = 1.0,
    retry_strategy: Literal["batch", "individual"] = "individual",
) -> list[dict]:
    """Wrapper around process_perspective_batch that handles partial failures with retries.

    This function implements exponential backoff retry logic for failed requests,
    supporting both batch and individual retry strategies while maintaining the
    original request order.

    Args:
        requests (list[dict]): List of Perspective API request payloads, same format
            as process_perspective_batch.
        max_retries (int): Maximum number of retry attempts for failed requests.
            Defaults to 4.
        initial_delay (float): Initial delay in seconds before first retry.
            Each subsequent retry doubles this delay. Defaults to 1.0.
        retry_strategy (Literal["batch", "individual"]): Strategy for handling retries:
            - "batch": Retries entire batch if any request fails
            - "individual": Retries only failed requests while maintaining order

    Returns:
        list[dict]: List of processed responses in the same order as input requests.
            Failed requests after all retries will be None.

    Control Flow:
        1. Attempts initial batch processing
        2. If retry_strategy is "batch":
            a. If any requests fail, retries entire batch with exponential backoff
            b. Continues until max_retries reached or all succeed
        3. If retry_strategy is "individual":
            a. Identifies failed requests by index
            b. Creates retry batches only for failed requests
            c. Retries with exponential backoff
            d. Merges successful retries back into original order
        4. Returns final results in original order

    Note:
        - Maintains request order regardless of retry strategy
        - Uses exponential backoff to prevent overwhelming the API
        - Logs retry attempts and success/failure counts
        - Returns None for requests that fail after all retries
    """
    if retry_strategy not in ["batch", "individual"]:
        raise ValueError(
            f"Invalid retry_strategy: {retry_strategy}. Must be either 'batch' or 'individual'."
        )

    if not requests:
        return []

    # Initial attempt
    responses: list[dict] = await process_perspective_batch(requests)
    current_delay = initial_delay
    attempt = 1

    if retry_strategy == "batch":
        # Retry entire batch if any requests failed
        while attempt < max_retries and None in responses:
            logger.warning(
                f"{len(responses) - responses.count(None)} successful, {responses.count(None)} failed requests. "
                f"Retrying entire batch (attempt {attempt + 1}/{max_retries})..."
            )
            await asyncio.sleep(current_delay)
            responses = await process_perspective_batch(requests)
            current_delay *= 2
            attempt += 1

    else:  # individual retry strategy
        # Track indices of failed requests for retrying
        failed_indices = [i for i, r in enumerate(responses) if r is None]

        while failed_indices and attempt < max_retries:
            logger.warning(
                f"{len(responses) - len(failed_indices)} successful, {len(failed_indices)} failed requests. "
                f"Retrying failed requests (attempt {attempt + 1}/{max_retries})..."
            )

            # Create retry batch with only failed requests
            retry_requests = [requests[i] for i in failed_indices]

            # Wait with exponential backoff
            await asyncio.sleep(current_delay)

            # Process retry batch
            retry_responses = await process_perspective_batch(retry_requests)

            # Update responses list with successful retries while maintaining order
            for original_idx, retry_response in zip(failed_indices, retry_responses):
                if retry_response is not None:
                    responses[original_idx] = retry_response

            # Update failed indices for next iteration
            failed_indices = [i for i, r in enumerate(responses) if r is None]
            current_delay *= 2
            attempt += 1

    # Log final results
    final_success_count = len([r for r in responses if r is not None])
    final_failure_count = len([r for r in responses if r is None])
    logger.info(
        f"Final results after retries: "
        f"{final_success_count} successful, {final_failure_count} failed"
    )

    return responses


def create_labels(posts: list[dict], responses: list[dict]) -> list[dict]:
    """Creates label models from posts and their corresponding API responses.

    This function pairs posts with their Perspective API responses to create label
    models. It handles various error cases and ensures each post gets a label model,
    even if the classification failed.

    Args:
        posts (list[dict]): List of posts to create labels for. Each post must contain:
            - uri (str): Unique identifier for the post
            - text (str): The text content that was classified
            - preprocessing_timestamp (str): Post creation timestamp in YYYY-MM-DD-HH:MM:SS format
        responses (list[dict]): List of Perspective API responses, one per post.
            Each response can be:
            - None: Indicating API call failed
            - dict with error: Indicating API returned error
            - dict with attributeScores: Successful API response

    Returns:
        list[dict]: List of serialized PerspectiveApiLabelsModel objects, one per
            post. Each model contains:
            - uri (str): Post identifier
            - text (str): Post content
            - preprocessing_timestamp (str): Post creation timestamp
            - was_successfully_labeled (bool): Whether classification succeeded
            - label_timestamp (str): Classification timestamp in YYYY-MM-DD-HH:MM:SS
            - reason (str): Error message if classification failed
            - prob_* (float): Probability scores for each attribute if successful
            All models are converted to dicts via model_dump()

    Control Flow:
        1. If no posts, returns empty list
        2. Validates response count matches post count
            - If mismatch, fills missing responses with None
        3. Gets current UTC timestamp for labeling
        4. For each post-response pair:
            a. If response exists and has attributeScores:
                - Validates required fields
                - If validation fails, marks as error
            b. For failed/error responses:
                - Creates failed label model with error reason
            c. For successful responses:
                - Extracts probability scores
                - Creates successful label model with scores
        5. Converts all models to dicts and returns

    Note:
        - Uses dynamic timestamps for precise tracking of when each batch was labeled
        - Handles all error cases gracefully
        - Only includes probability scores in successful models, not binary labels
        - All responses must be processed, even if they represent failures
    """
    if not posts:
        return []

    if len(responses) != len(posts):
        logger.warning(
            f"Number of responses ({len(responses)}) does not match number of posts ({len(posts)}). Likely means that some posts failed to be labeled. Re-inserting all posts into queue..."
        )
        responses = [None] * len(posts)

    res = []

    # the reason why I define the timestamp dynamically instead
    # of setting this up as a single value for the entire job,
    # like I do elsewhere, is that inference can take hours to
    # run and is batched. Therefore, I wanto to have a more
    # fine-grained idea of when a specific sample was
    # classified, not just the single timestamp that the job
    # was started.
    label_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H:%M:%S")

    for post, response_obj in zip(posts, responses):
        # Check for malformed response
        if response_obj is not None and "attributeScores" in response_obj:
            is_malformed = False
            error_msg = []

            # Validate required fields
            for attribute in response_obj["attributeScores"]:
                if "summaryScore" not in response_obj["attributeScores"][attribute]:
                    is_malformed = True
                    error_msg.append(
                        f"Missing required field: summaryScore for {attribute}"
                    )
                elif (
                    "value"
                    not in response_obj["attributeScores"][attribute]["summaryScore"]
                ):
                    is_malformed = True
                    error_msg.append(
                        f"Missing required field: value in summaryScore for {attribute}"
                    )

            if is_malformed:
                response_obj = {"error": "; ".join(error_msg)}

        if response_obj is None or "error" in response_obj:
            if response_obj is None:
                response_obj = {"error": "No response from Perspective API"}
            print(
                f"Error processing post {post['uri']} using the Perspective API: {response_obj['error']}"
            )
            res.append(
                PerspectiveApiLabelsModel(
                    uri=post["uri"],
                    text=post["text"],
                    preprocessing_timestamp=post["preprocessing_timestamp"],
                    was_successfully_labeled=False,
                    reason=response_obj["error"],
                    label_timestamp=label_timestamp,
                )
            )
        else:
            # we only want the probs, not the labels, since we want to enforce
            # our own threshold for what constitutes a label
            probs_response_obj = {
                field: value
                for (field, value) in response_obj.items()
                if field.startswith("prob_")
            }
            try:
                res.append(
                    PerspectiveApiLabelsModel(
                        uri=post["uri"],
                        text=post["text"],
                        preprocessing_timestamp=post["preprocessing_timestamp"],
                        was_successfully_labeled=True,
                        label_timestamp=label_timestamp,
                        **probs_response_obj,
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Unable to export the following record ({post}) and label ({response_obj}), due to error ({e})"
                )
                res.append(
                    PerspectiveApiLabelsModel(
                        uri=post["uri"],
                        text=post["text"],
                        preprocessing_timestamp=post["preprocessing_timestamp"],
                        was_successfully_labeled=False,
                        reason=str(e),
                        label_timestamp=label_timestamp,
                    )
                )
    return [label.model_dump() for label in res]


@track_performance
async def batch_classify_posts(
    posts: list[dict],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = 1.0,
    retry_strategy: Literal["batch", "individual"] = "individual",
    max_retries: int = 3,
    initial_retry_delay: float = 1.0,
) -> dict:
    """Asynchronously processes and classifies posts in batches using the Perspective API.

    This function handles the core batch processing logic, including request creation,
    batching, API calls, and result management. It processes posts in batches to respect
    API rate limits and manages memory efficiently through garbage collection.

    Args:
        posts (list[dict]): List of posts to classify. Each post must contain:
            - text (str): The text content to classify
            - uri (str): Unique identifier for the post
            - preprocessing_timestamp (str): Post creation timestamp in YYYY-MM-DD-HH:MM:SS format
            - batch_id: ID linking the post to its original batch in the queue
        batch_size (Optional[int]): Number of posts to process in each API batch.
            Defaults to DEFAULT_BATCH_SIZE (90). Should not exceed 100 due to
            Perspective API QPS limits.
        seconds_delay_per_batch (Optional[float]): Delay between batch processing
            to respect rate limits. Defaults to 1.0 seconds.
        retry_strategy (Literal["batch", "individual"]): Strategy for handling retries:
            - "batch": Retries entire batch if any request fails
            - "individual": Retries only failed requests while maintaining order
        max_retries (int): Maximum number of retry attempts for failed requests.
            Defaults to 3.
        initial_retry_delay (float): Initial delay in seconds before first retry.
            Each subsequent retry doubles this delay. Defaults to 1.0.

    Returns:
        dict: Classification summary containing:
            - total_batches (int): Number of batches processed
            - total_posts_successfully_labeled (int): Count of posts successfully labeled
            - total_posts_failed_to_label (int): Count of posts that failed classification

    Control Flow:
        1. Creates Perspective API request payloads for each post
        2. Pairs original posts with their request payloads
        3. Splits paired data into batches of specified size
        4. For each batch:
            a. Processes posts through Perspective API in parallel with retry logic
            b. Applies rate limiting delay between batches
            c. Creates label models from responses
            d. Separates successful and failed labels
            e. For successful labels:
                - Writes to cache via write_posts_to_cache()
                - Updates success counter
            f. For failed labels:
                - Returns to input queue via return_failed_labels_to_input_queue()
                - Updates failure counter
            g. Performs memory cleanup via garbage collection
        5. Returns summary statistics

    Note:
        - Uses asyncio for concurrent processing but maintains rate limits
        - Failed classifications are automatically requeued for retry
        - Memory is actively managed through explicit cleanup after each batch
        - Progress is logged every 50 batches
        - Implements exponential backoff for retries
        - Supports both batch and individual retry strategies
    """
    request_payloads: list[dict] = [
        create_perspective_request(post["text"]) for post in posts
    ]
    iterated_post_payloads: list[tuple[dict, dict]] = list(zip(posts, request_payloads))
    batches: list[list[tuple[dict, dict]]] = create_batches(
        batch_list=iterated_post_payloads, batch_size=batch_size
    )
    total_batches = len(batches)
    total_posts_successfully_labeled = 0
    total_posts_failed_to_label = 0

    for i, post_request_batch in enumerate(batches):
        if i % 50 == 0:
            logger.info(f"Processing batch {i}/{total_batches}")

        post_batch, request_batch = zip(*post_request_batch)

        responses: list[dict] = await process_perspective_batch_with_retries(
            requests=request_batch,
            max_retries=max_retries,
            initial_delay=initial_retry_delay,
            retry_strategy=retry_strategy,
        )

        # Apply rate limiting delay between batches
        await asyncio.sleep(seconds_delay_per_batch)

        labels: list[dict] = create_labels(posts=post_batch, responses=responses)

        successful_labels: list[dict] = []
        failed_labels: list[dict] = []

        # Process labels and update counters
        for post, label in zip(post_batch, labels):
            post_batch_id = post["batch_id"]
            label["batch_id"] = post_batch_id
            if label["was_successfully_labeled"]:
                successful_labels.append(label)
            else:
                failed_labels.append(label)

        # Handle successful labels
        if successful_labels:
            logger.info(f"Successfully labeled {len(successful_labels)} posts.")
            write_posts_to_cache(
                inference_type="perspective_api",
                posts=successful_labels,
                batch_size=batch_size,
            )
            total_posts_successfully_labeled += len(successful_labels)

        # Handle failed labels
        if failed_labels:
            logger.error(
                f"Failed to label {len(failed_labels)} posts after all retries. "
                "Re-inserting these into queue."
            )
            return_failed_labels_to_input_queue(
                inference_type="perspective_api",
                failed_label_models=failed_labels,
                batch_size=batch_size,
            )
            total_posts_failed_to_label += len(failed_labels)

        # Memory cleanup
        del successful_labels
        del failed_labels
        del post_batch
        del request_batch
        del responses
        del labels
        gc.collect()

    return {
        "total_batches": total_batches,
        "total_posts_successfully_labeled": total_posts_successfully_labeled,
        "total_posts_failed_to_label": total_posts_failed_to_label,
    }


def run_batch_classification(
    posts: list[dict],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS,
) -> dict:
    """Orchestrates batch classification of posts using the Perspective API.

    This function is the main entry point for batch classification. It handles the
    asynchronous processing of posts through the Perspective API, including batching,
    rate limiting, and result management.

    Args:
        posts (list[dict]): List of posts to classify. Each post must contain:
            - text (str): The text content to classify
            - uri (str): Unique identifier for the post
            - preprocessing_timestamp (str): Post creation timestamp
            - batch_id: ID linking the post to its original batch in the queue
        batch_size (Optional[int]): Number of posts to process in each API batch.
            Defaults to DEFAULT_BATCH_SIZE (90). Should not exceed 100 due to
            Perspective API QPS limits.
        seconds_delay_per_batch (Optional[float]): Delay between batch processing
            to respect rate limits. Defaults to DEFAULT_DELAY_SECONDS (1.05).

    Returns:
        dict: Classification metadata containing:
            - total_batches (int): Number of batches processed
            - total_posts_successfully_labeled (int): Count of posts successfully labeled
            - total_posts_failed_to_label (int): Count of posts that failed classification

    Control Flow:
        1. Creates event loop for async execution
        2. Runs batch_classify_posts() which:
            a. Creates Perspective API request payloads for each post
            b. Splits posts into batches of specified size
            c. For each batch:
                - Processes posts through Perspective API
                - Creates label models from responses
                - Writes successful labels to cache
                - Returns failed labels to input queue
                - Applies rate limiting delay
            d. Tracks success/failure counts
        3. Returns classification metadata

    Note:
        The function uses asyncio for concurrent processing but maintains rate limits
        through explicit delays. Failed classifications are automatically requeued
        for retry.
    """
    loop = asyncio.get_event_loop()
    metadata = loop.run_until_complete(
        batch_classify_posts(
            posts=posts,
            batch_size=batch_size,
            seconds_delay_per_batch=seconds_delay_per_batch,
        )
    )
    return metadata
