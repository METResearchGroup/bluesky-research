"""Base file for classifying posts in batch for sociopolitical characteristics
using LLMs.

For LLM classification, there is a limit to how much we can batch at the same
time due to compute constraints, so we need to classify in batches and we'll
be more restrictive about the posts that will be classified as compared to the
Perspective API classification.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import json
from typing import Literal, Optional

from langchain.output_parsers import RetryOutputParser
from langchain_core.output_parsers import JsonOutputParser
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
from pydantic import ValidationError

from lib.constants import current_datetime_str
from lib.db.sql.ml_inference_database import (
    batch_insert_metadata, batch_insert_sociopolitical_labels,
    get_existing_sociopolitical_uris
)
from lib.db.sql.preprocessing_database import get_filtered_posts
from lib.helper import create_batches, track_performance
from ml_tooling.llm.model import (
    DEFAULT_BATCH_SIZE, DEFAULT_DELAY_SECONDS, DEFAULT_TASK_NAME,
    LLM_MODEL_NAME, get_llm_model
)
from ml_tooling.llm.task_prompts import (
    single_text_explanation_prompt, task_name_to_task_prompt_map
)
from pipelines.classify_records.helper import (
    get_post_metadata_for_classification, validate_posts
)
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel, RecordClassificationMetadataModel,
    SociopoliticalLabelsModel
)
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel


# TODO: change to local=True for testing.
llm_model = get_llm_model(local=True)
parser = JsonOutputParser(pydantic_object=LLMSociopoliticalLabelModel)
retry_parser = RetryOutputParser.from_llm(parser=parser, llm=llm_model)


def load_posts_to_classify(
    num_days_lookback: Optional[int] = 1,
    feed_source: Optional[Literal["firehose", "most_liked"]] = None
) -> list[FilteredPreprocessedPostModel]:
    """Load posts for Perspective API classification. Load only the posts that
    haven't been classified yet.
    """
    if num_days_lookback:
        latest_preprocessing_timestamp = (
            datetime.now(timezone.utc) - timedelta(days=num_days_lookback)
        ).strftime("%Y-%m-%d")
    else:
        latest_preprocessing_timestamp = None
    preprocessed_posts: list[FilteredPreprocessedPostModel] = get_filtered_posts(  # noqa
        latest_preprocessing_timestamp=latest_preprocessing_timestamp,
        feed_source=feed_source
    )
    existing_uris: set[str] = get_existing_sociopolitical_uris(
        latest_preprocessing_timestamp=latest_preprocessing_timestamp,
        feed_source=feed_source
    )
    if not existing_uris:
        return preprocessed_posts
    posts = [
        post for post in preprocessed_posts if post.uri not in existing_uris
    ]
    # sort by synctimestamp ascending so the oldest posts are first.
    sorted_posts = sorted(posts, key=lambda x: x.synctimestamp, reverse=False)
    return sorted_posts


def run_chain(
    post: RecordClassificationMetadataModel,
    model=llm_model,
    task_name: Optional[str] = DEFAULT_TASK_NAME
) -> LLMSociopoliticalLabelModel:
    """Create chain to run inference task."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    full_prompt = f"""
{task_prompt}
{single_text_explanation_prompt}
    """
    langchain_prompt = PromptTemplate(
        template=f"{full_prompt}\n" + "{text}" + "{format_instructions}",
        input_variables=["text"],
        partial_variables={"format_instructions": parser.get_format_instructions()},  # noqa
    )
    chain = LLMChain(prompt=langchain_prompt, llm=model, output_parser=parser)
    try:
        result: dict = chain.invoke({"text": post.text})
    except (ValidationError, ValueError, json.JSONDecodeError) as e:
        # Langchain will try to validate the response and in the case where
        # the output format is incorrect, we can retry.
        print(f"Error decoding JSON response: {e}")
        print(f"Retrying with formatted prompt.")
        formatted_prompt = langchain_prompt.format_prompt(text=post.text)
        result = retry_parser.parse_with_prompt(result, formatted_prompt)
    model = LLMSociopoliticalLabelModel(
        is_sociopolitical=result["text"]["is_sociopolitical"],
        political_ideology_label=result["text"]["political_ideology_label"]
    )
    return model


def export_validated_llm_output(
    post: RecordClassificationMetadataModel,
    result: LLMSociopoliticalLabelModel,
) -> SociopoliticalLabelsModel:
    """Write the validated LLM output to the database."""
    label_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H:%M:%S")
    output_model = (
        SociopoliticalLabelsModel(
            uri=post.uri,
            text=post.text,
            llm_model_name=LLM_MODEL_NAME,
            was_successfully_labeled=True,
            label_timestamp=label_timestamp,
            is_sociopolitical=result.is_sociopolitical,
            political_ideology_label=result.political_ideology_label,
        )
    )
    batch_insert_sociopolitical_labels(labels=[output_model])
    return output_model


def process_llm_batch(
    post_batch: list[RecordClassificationMetadataModel],
    max_retries: Optional[int] = 5
) -> dict:
    """Process a batch of posts and prompt using the LLM.

    Retries up to `max_retries` times if the output is invalid. If the output
    is valid, then attempts to write the result.

    Returns a dict based on if the processing failed. If failed, returns the
    post batch and prompt to retry later.
    """
    inserted_results: list[LLMSociopoliticalLabelModel] = []
    for post in post_batch:
        validated_result = False
        num_retries = 0
        while not validated_result and num_retries < max_retries:
            result: LLMSociopoliticalLabelModel = run_chain(post=post)
            if result is not None:
                print("LLM output validated. Now trying to write to DB.")
                output_model: SociopoliticalLabelsModel = (
                    export_validated_llm_output(post=post, result=result)
                )
                print("Successfully validated LLM output and results.")
                validated_result = True
                inserted_results.append(output_model)
            num_retries += 1
        # TODO: need to think about what happens once we have max retries
        if num_retries >= max_retries:
            print(f"Failed to validate LLM output after maximum {num_retries} retries.")  # noqa
            return {"succeeded": False, "response": post_batch}
    return {"succeeded": True, "response": inserted_results}


@track_performance
async def batch_classify_posts(
    posts: list[RecordClassificationMetadataModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS,
) -> list[SociopoliticalLabelsModel]:
    """Classify posts in batches."""
    post_batches: list[list[RecordClassificationMetadataModel]] = (
        create_batches(batch_list=posts, batch_size=batch_size)
    )
    successful_batches: list[SociopoliticalLabelsModel] = []
    failed_batches: list[tuple] = []
    for batch in post_batches:
        result_dict = process_llm_batch(post_batch=batch)
        await asyncio.sleep(seconds_delay_per_batch)
        if result_dict["succeeded"]:
            successful_batches.extend(result_dict["response"])
        else:
            failed_batches.extend(result_dict["response"])

    for post_batch in failed_batches:
        result_dict = process_llm_batch(post_batch=post_batch)
        if not result_dict["succeeded"]:
            # TODO: not sure what to do if it still fails after so many tries?
            print("Failed to process batch after retrying for 2 iterations of batched inference.")  # noqa
    return successful_batches


@track_performance
def run_batch_classification(
    posts: list[RecordClassificationMetadataModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS
) -> list[SociopoliticalLabelsModel]:
    loop = asyncio.get_event_loop()
    results: list[SociopoliticalLabelsModel] = loop.run_until_complete(
        batch_classify_posts(
            posts=posts,
            batch_size=batch_size,
            seconds_delay_per_batch=seconds_delay_per_batch,
        )
    )
    return results


def classify_latest_posts():
    # load posts
    posts: list[FilteredPreprocessedPostModel] = load_posts_to_classify()
    print(f"Number of posts loaded for sociopolitical classification: {len(posts)}")  # noqa

    # fetch metadata of posts to be classified. Insert the metadata to DBs
    # (if not already present)
    posts_to_classify: list[RecordClassificationMetadataModel] = (
        get_post_metadata_for_classification(posts)
    )
    batch_insert_metadata(posts_to_classify)

    # validate posts
    valid_posts, invalid_posts = validate_posts(posts_to_classify)
    print(f"Number of valid posts: {len(valid_posts)}")
    print(f"Number of invalid posts: {len(invalid_posts)}")
    print(f"Classifying {len(valid_posts)} posts for sociopolitical characteristics, via LLMs...")  # noqa
    print(f"Defaulting {len(invalid_posts)} posts to failed label...")

    # insert invalid posts into DB first, before running LLM sociopolitical
    # classification
    invalid_posts_models = []
    for post in invalid_posts:
        invalid_posts_models.append(
            SociopoliticalLabelsModel(
                uri=post["uri"],
                text=post["text"],
                was_successfully_labeled=False,
                reason="text_too_short",
                label_timestamp=current_datetime_str,
            )
        )

    print(f"Inserting {len(invalid_posts_models)} invalid posts into the DB.")
    batch_insert_sociopolitical_labels(invalid_posts_models)
    print(f"Completed inserting {len(invalid_posts_models)} invalid posts into the DB.")  # noqa

    # run inference on valid posts
    print(f"Running batch classification on {len(valid_posts)} valid posts.")
    run_batch_classification(posts=valid_posts)
    print("Completed batch classification.")


if __name__ == "__main__":
    default_fields = {
        "text": "<test_text>",
        "synctimestamp": "2021-09-01-12:00:00",
        "preprocessing_timestamp": "2021-09-01-12:00:00",
        "source": "firehose",
        "url": None,
        "like_count": None,
        "reply_count": None,
        "repost_count": None,
    }
    num_repetitions = 1
    test_texts = [
        "I think that the government should be more involved in the economy.",
        "I think that the government should be less involved in the economy.",
        "I think that the government should be more involved in social issues.",
        "I think that the government should be less involved in social issues.",
        "I think that the government should be more involved in foreign policy.",
        "I think that the government should be less involved in foreign policy.",
        "I think that the government should be more involved in the environment.",
        "I think that the government should be less involved in the environment.",
    ] * num_repetitions
    test_posts = [
        RecordClassificationMetadataModel(
            **{
                **default_fields,
                "text": text,
                "uri": f"<test_uri__{current_datetime_str}_{i}>"
            }
        )
        for i, text in enumerate(test_texts)
    ]
    results = run_batch_classification(posts=test_posts)

    # log results
    result_dicts = [result.dict() for result in results]
    results_str = "\n".join([str(result) for result in result_dicts])
    print(f"Results:\n{results_str}")
