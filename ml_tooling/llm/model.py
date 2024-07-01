"""Model for classifying posts using LLMs."""
import asyncio
from datetime import datetime, timezone
from typing import Optional

from langchain.chains import LLMChain
from langchain_core.output_parsers import JsonOutputParser
from langchain_community.chat_models import ChatLiteLLM
from langchain_core.prompts import PromptTemplate


from lib.db.sql.ml_inference_database import batch_insert_sociopolitical_labels
from lib.helper import create_batches, track_performance
from ml_tooling.llm.inference import BACKEND_OPTIONS, async_run_query
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


def run_chain(
    post: RecordClassificationMetadataModel,
    model: ChatLiteLLM = model,
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
    chain = LLMChain(
        prompt=langchain_prompt,
        llm=model,
        output_parser=parser
    )
    result: dict = chain.invoke({"text": post.text})
    # TODO: update chaining to add retries where necessary
    # see, for example, https://python.langchain.com/v0.1/docs/modules/model_io/output_parsers/types/retry/
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
    output_model = [
        SociopoliticalLabelsModel(
            uri=post.uri,
            text=post.text,
            llm_model_name=LLM_MODEL_NAME,
            was_successfully_labeled=True,
            label_timestamp=label_timestamp,
            is_sociopolitical=result.is_sociopolitical,
            political_ideology_label=result.political_ideology_label,
        )
    ]
    batch_insert_sociopolitical_labels(labels=output_model)
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

    test_texts = [
        "I think that the government should be more involved in the economy.",
        "I think that the government should be less involved in the economy.",
        "I think that the government should be more involved in social issues.",
        "I think that the government should be less involved in social issues.",
        "I think that the government should be more involved in foreign policy.",
        "I think that the government should be less involved in foreign policy.",
        "I think that the government should be more involved in the environment.",
        "I think that the government should be less involved in the environment.",
    ]
    test_posts = [
        RecordClassificationMetadataModel(
            **{**default_fields, "text": text, "uri": f"<test_uri_{i}>"}
        )
        for i, text in enumerate(test_texts)
    ]
    results = run_batch_classification(posts=test_posts)
