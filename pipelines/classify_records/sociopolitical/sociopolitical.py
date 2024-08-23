"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""

import asyncio
from datetime import datetime, timezone
import json
from typing import Literal, Optional

from langchain.output_parsers import RetryOutputParser
from langchain_core.output_parsers import JsonOutputParser

from lib.constants import current_datetime_str
from lib.helper import create_batches, track_performance
from lib.log.logger import get_logger
from ml_tooling.llm.model import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DELAY_SECONDS,
    LLM_MODEL_NAME,
    get_llm_model,
)
from services.ml_inference.helper import get_posts_to_classify, insert_labeling_session  # noqa
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel,
    SociopoliticalLabelsModel,
)
from services.ml_inference.sociopolitical.export_data import (
    export_results,
    write_post_to_cache,
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

logger = get_logger(__name__)

# TODO: change to local=True for testing.
llm_model = get_llm_model(local=False)
parser = JsonOutputParser(pydantic_object=LLMSociopoliticalLabelModel)
retry_parser = RetryOutputParser.from_llm(parser=parser, llm=llm_model)


# @traceable
# def run_chain(
#     post: FilteredPreprocessedPostModel,
#     model=llm_model,
#     task_name: Optional[str] = DEFAULT_TASK_NAME,
# ) -> LLMSociopoliticalLabelModel:
#     """Create chain to run inference task."""
#     task_prompt = task_name_to_task_prompt_map[task_name]
#     full_prompt = f"""
# {task_prompt}
# {single_text_explanation_prompt}
#     """
#     langchain_prompt = PromptTemplate(
#         template=f"{full_prompt}\n" + "{text}" + "{format_instructions}",
#         input_variables=["text"],
#         partial_variables={"format_instructions": parser.get_format_instructions()},  # noqa
#     )
#     chain = LLMChain(prompt=langchain_prompt, llm=model, output_parser=parser)
#     try:
#         result: dict = chain.invoke({"text": post.text})
#     except (ValidationError, ValueError, json.JSONDecodeError) as e:
#         # Langchain will try to validate the response and in the case where
#         # the output format is incorrect, we can retry.
#         print(f"Error decoding JSON response: {e}")
#         print("Retrying with formatted prompt.")
#         formatted_prompt = langchain_prompt.format_prompt(text=post.text)
#         result = retry_parser.parse_with_prompt(result, formatted_prompt)
#     model = LLMSociopoliticalLabelModel(
#         is_sociopolitical=result["text"]["is_sociopolitical"],
#         political_ideology_label=result["text"]["political_ideology_label"],
#     )
#     return model


# TODO: implement.
def generate_prompt(posts: list[FilteredPreprocessedPostModel]) -> str:
    """Generates a prompt for the LLM."""
    pass


# TODO: implement.
def run_inference(prompt: str) -> str:
    """Runs inference for a given prompt."""
    pass


# TODO: implement
def parse_llm_result(json_result: str) -> list[LLMSociopoliticalLabelModel]:  # noqa
    results = []
    for line in json_result.strip().split("\n"):
        try:
            result = json.loads(line)
            result_model = LLMSociopoliticalLabelModel(**result)
            results.append(result_model)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON line: {e}")
            continue


# TODO: implement.
def process_sociopolitical_batch(
    posts: list[FilteredPreprocessedPostModel],
    retry_count: int = 0,
    max_retries: Optional[int] = 5,
) -> list[LLMSociopoliticalLabelModel]:
    """Takes batch and runs the LLM for it."""
    # TODO: still need output validation somehow, but don't want to
    # overcomplicate things by using chains.
    prompt: str = generate_prompt(posts)
    json_result: str = run_inference(prompt)
    # Parse the JSON lines string into a list of dictionaries
    results: list[LLMSociopoliticalLabelModel] = parse_llm_result(json_result)
    if len(results) != len(posts):
        # TODO: need to raise error and retry.
        raise ValueError(
            f"Number of results ({len(results)}) does not match number of posts ({len(posts)})."
        )
    return results


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
    max_retries: Optional[int] = 5,
) -> dict:
    """Process a batch of posts and prompt using the LLM.

    Retries up to `max_retries` times if the output is invalid. If the output
    is valid, then attempts to write the result.

    Returns a dict based on if the processing failed. If failed, returns the
    post batch and prompt to retry later.
    """
    inserted_results: list[LLMSociopoliticalLabelModel] = []
    results: list[LLMSociopoliticalLabelModel] = process_sociopolitical_batch(
        posts=post_batch, max_retries=max_retries
    )  # noqa
    output_models: list[SociopoliticalLabelsModel] = export_validated_llm_output(  # noqa
        posts=post_batch, results=results, source_feed=source_feed
    )
    inserted_results.extend(output_models)
    return {"succeeded": True, "response": inserted_results}


@track_performance
async def batch_classify_posts(
    posts: list[FilteredPreprocessedPostModel],
    source_feed: Literal["firehose", "most_liked"],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS,
) -> list[SociopoliticalLabelsModel]:
    """Classify posts in batches."""
    post_batches: list[list[FilteredPreprocessedPostModel]] = create_batches(
        batch_list=posts, batch_size=batch_size
    )
    successful_batches: list[SociopoliticalLabelsModel] = []
    failed_batches: list[tuple] = []
    for batch in post_batches:
        result_dict = process_llm_batch(post_batch=batch, source_feed=source_feed)  # noqa
        await asyncio.sleep(seconds_delay_per_batch)
        if result_dict["succeeded"]:
            successful_batches.extend(result_dict["response"])
        else:
            failed_batches.extend(result_dict["response"])

    for post_batch in failed_batches:
        result_dict = process_llm_batch(post_batch=post_batch, source_feed=source_feed)  # noqa
        if not result_dict["succeeded"]:
            # TODO: not sure what to do if it still fails after so many tries?
            print(
                "Failed to process batch after retrying for 2 iterations of batched inference."
            )  # noqa
    return successful_batches


@track_performance
def run_batch_classification(
    posts: list[FilteredPreprocessedPostModel],
    source_feed: Literal["firehose", "most_liked"],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS,
) -> list[SociopoliticalLabelsModel]:
    loop = asyncio.get_event_loop()
    results: list[SociopoliticalLabelsModel] = loop.run_until_complete(
        batch_classify_posts(
            posts=posts,
            source_feed=source_feed,
            batch_size=batch_size,
            seconds_delay_per_batch=seconds_delay_per_batch,
        )
    )
    return results


def classify_latest_posts():
    """Classifies the latest posts using LLM inference.

    NOTE: for now we're just using an LLM. Would be nice to eventually use a
    fine-tuned BERT model, but we'll revisit this later.
    """
    labeling_session = {
        "inference_type": "llm",
        "inference_timestamp": current_datetime_str,
    }
    posts_to_classify: list[FilteredPreprocessedPostModel] = get_posts_to_classify(  # noqa
        inference_type="llm"
    )
    logger.info(
        f"Classifying {len(posts_to_classify)} posts with the Perspective API..."
    )  # noqa
    if len(posts_to_classify) == 0:
        logger.warning("No posts to classify with LLM. Exiting...")
        return
    firehose_posts = [post for post in posts_to_classify if post.source == "firehose"]
    most_liked_posts = [
        post for post in posts_to_classify if post.source == "most_liked"
    ]

    source_to_posts_tuples = [
        ("firehose", firehose_posts),
        ("most_liked", most_liked_posts),
    ]  # noqa
    for source, posts in source_to_posts_tuples:
        # labels stored in local storage, and then loaded
        # later. This format is done to make it more
        # robust to errors and to the script failing (though
        # tbh I could probably just return the posts directly
        # and then write to S3).
        run_batch_classification(posts=posts, source_feed=source)
    results = export_results(
        current_timestamp=current_datetime_str, external_stores=["s3"]
    )
    labeling_session = {
        "inference_type": "perspective_api",
        "inference_timestamp": current_datetime_str,
        "total_classified_posts": results["total_classified_posts"],
        "total_classified_posts_by_source": results["total_classified_posts_by_source"],  # noqa
    }
    insert_labeling_session(labeling_session)


if __name__ == "__main__":
    num_repetitions = 1
    test_texts = [
        "I think that the government should be more involved in the economy.",
        "I think that the government should be less involved in the economy.",
        "I think that the government should be more involved in social issues.",  # noqa
        "I think that the government should be less involved in social issues.",  # noqa
        "I think that the government should be more involved in foreign policy.",  # noqa
        "I think that the government should be less involved in foreign policy.",  # noqa
        "I think that the government should be more involved in the environment.",  # noqa
        "I think that the government should be less involved in the environment.",  # noqa
    ] * num_repetitions
    # TODO: just test the text inference portion.
