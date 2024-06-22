"""Model for classifying posts using LLMs."""
import asyncio
from datetime import datetime, timezone
from typing import Literal, Optional
import yaml

from langchain_core.output_parsers import JsonOutputParser
from langchain_community.chat_models import ChatLiteLLM
from langchain_core.prompts import PromptTemplate


from lib.db.sql.ml_inference_database import batch_insert_sociopolitical_labels
from lib.helper import create_batches, track_performance
from ml_tooling.llm.inference import (
    BACKEND_OPTIONS, async_run_query, num_tokens_from_string
)
from ml_tooling.llm.task_prompts import (
    single_text_explanation_prompt, task_name_to_task_prompt_map
)
from services.ml_inference.models import (
    LLMSociopoliticalLabelModel, RecordClassificationMetadataModel,
    SociopoliticalLabelsModel
)

DEFAULT_BATCH_SIZE = 10  # TODO: should be # of posts in a prompt. 50 is a guess.
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TASK_NAME = "civic_and_political_ideology"
LLM_MODEL_NAME = BACKEND_OPTIONS["Gemini"]["model"]  # "gemini/gemini-pro"

model = ChatLiteLLM(model="gemini/gemini-1.0-pro-latest")
parser = JsonOutputParser(pydantic_object=LLMSociopoliticalLabelModel)


def generate_single_post_prompt(
    post: RecordClassificationMetadataModel,
    task_name: Optional[str] = DEFAULT_TASK_NAME
) -> str:
    """Create a prompt that classifies a single post."""
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
    chain = langchain_prompt | model | parser
    token_count = num_tokens_from_string(full_prompt)
    print(f"Token count for prompt: {token_count}")
    result = chain.invoke({"text": post.text})
    return result


def export_validated_llm_output(
    post: RecordClassificationMetadataModel,
    result: LLMSociopoliticalLabelModel,
):
    """Write the validated LLM output to the database."""
    label_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H:%M:%S")
    output_models = (
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
    batch_insert_sociopolitical_labels(output_models)


async def process_llm_batch(
    post_batch: list[RecordClassificationMetadataModel],
    max_retries: Optional[int] = 5
) -> dict:
    """Process a batch of posts and prompt using the LLM.

    Retries up to `max_retries` times if the output is invalid. If the output
    is valid, then attempts to write the result.

    Returns a dict based on if the processing failed. If failed, returns the
    post batch and prompt to retry later.
    """
    for post in post_batch:
        validated_result = None
        num_retries = 0
        while validated_result is None and num_retries < max_retries:
            prompt = generate_single_post_prompt(post=post)
            result: str = await async_run_query(prompt=prompt)
            if result is not None:
                print("LLM output validated. Now trying to write to DB.")
                export_validated_llm_output(posts=post, result=result)
                print("Successfully validated LLM output and results.")
            num_retries += 1
        # TODO: need to think about what happens once we have max retries
        if num_retries >= max_retries:
            print(f"Failed to validate LLM output after maximum {num_retries} retries.")  # noqa
            return {
                "succeeded": False,
                "response": [post_batch]
            }
    return {
        "succeeded": True, "response": []
    }


@track_performance
async def batch_classify_posts(
    posts: list[RecordClassificationMetadataModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS,
):
    """Classify posts in batches."""
    post_batches: list[list[RecordClassificationMetadataModel]] = (
        create_batches(batch_list=posts, batch_size=batch_size)
    )
    failed_batches: list[tuple] = []
    for batch in post_batches:
        result_dict = await process_llm_batch(post_batch=batch)
        await asyncio.sleep(seconds_delay_per_batch)
        if not result_dict["succeeded"]:
            failed_batches.append(result_dict["response"])

    for post_batch in failed_batches:
        result_dict = process_llm_batch(post_batch=post_batch)
        if not result_dict["succeeded"]:
            # TODO: not sure what to do if it still fails after so many tries?
            print("Failed to process batch after retrying for 2 iterations of batched inference.")  # noqa


@track_performance
def run_batch_classification(
    posts: list[RecordClassificationMetadataModel],
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float] = DEFAULT_DELAY_SECONDS
):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        batch_classify_posts(
            posts=posts,
            batch_size=batch_size,
            seconds_delay_per_batch=seconds_delay_per_batch,
        )
    )
