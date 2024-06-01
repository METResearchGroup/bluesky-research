"""Model for classifying posts using LLMs."""
import asyncio
from datetime import datetime, timezone
from typing import Optional
import yaml

from langchain.output_parsers.yaml import YamlOutputParser
from langchain_community.chat_models import ChatLiteLLM
from langchain_core.prompts import PromptTemplate

from lib.db.sql.ml_inference_database import batch_insert_sociopolitical_labels
from lib.helper import create_batches, track_performance
from ml_tooling.llm.inference import (
    BACKEND_OPTIONS, async_run_query, num_tokens_from_string
)
from ml_tooling.llm.task_prompts import task_name_to_task_prompt_map
from services.ml_inference.models import (
    LLMSociopoliticalOutputModel, RecordClassificationMetadataModel,
    SociopoliticalLabelsModel
)

DEFAULT_BATCH_SIZE = 10 # TODO: should be # of posts in a prompt. 50 is a guess.
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TASK_NAME = "civic_and_political_ideology"
LLM_MODEL_NAME = BACKEND_OPTIONS["Gemini"]["model"] # "gemini/gemini-pro"

model = ChatLiteLLM(model="gemini/gemini-1.0-pro-latest")
parser = YamlOutputParser(pydantic_object=LLMSociopoliticalOutputModel)


def generate_batched_post_prompt(
    posts: list[RecordClassificationMetadataModel],
    task_name: Optional[str]=DEFAULT_TASK_NAME
) -> list[str]:
    """Create a prompt that classifies a batch of posts."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    task_prompt += """
You will receive an enumerated list of texts to classify, under <TEXTS>

Return a YAML with the following format. The format must be returned as valid YAML. Do not return any backticks or triple quotes or ```yaml``` code blocks.
Just return the raw string in YAML format without any additional formatting or markdown.
I will be loading the YAML as a string in Python, so the answer must be a string that can be processed by `yaml.safe_load`.

For example, for a list of 10 texts, the format must be in this format. 
```
results:
    - result_1:
        - is_sociopolitical: <is_sociopolitical for post 1>
        - political_ideology_label: <political_ideology_label for post 1>
        - reason_sociopolitical: <reason_sociopolitical for post 1>
        - reason_political_ideology: <reason_political_ideology for post 1>
    - result_2:
        - is_sociopolitical: <is_sociopolitical for post 2>
        - political_ideology_label: <political_ideology_label for post 2>
        - reason_sociopolitical: <reason_sociopolitical for post 2>
        - reason_political_ideology: <reason_political_ideology for post 2>
    ...
    - result_10:
        - is_sociopolitical: <is_sociopolitical for post 10>
        - political_ideology_label: <political_ideology_label for post 10>
        - reason_sociopolitical: <reason_sociopolitical for post 10>
        - reason_political_ideology: <reason_political_ideology for post 10>
count: 10
```

<TEXTS>
{texts}
    """ # noqa
    texts = [post.text for post in posts]
    enumerated_texts = "\n".join([f"{i}. {text}" for i, text in enumerate(texts, start=1)])

    # TODO: this appears to work, but once it does, I'll have to move it to
    # better fit the logic of this code, especially since this function is
    # supposed to just generate the prompt. I'll run the chain later, in lieu
    # of async_run_query.
    langchain_prompt = PromptTemplate(
        template=f"{task_prompt}\n" + "{format_instructions}",
        input_variables=["texts"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    breakpoint() # TODO: check prompt

    # chain = langchain_prompt | model | parser
    chain = langchain_prompt | model
    result = chain.invoke({"texts": texts})

    breakpoint() # TODO: check result
    full_prompt = f"""
{task_prompt}

{enumerated_texts}
    """
    # token_count = num_tokens_from_string(full_prompt)
    # print(f"Token count for prompt: {token_count}")
    return full_prompt


def validated_llm_result(
    posts: list[RecordClassificationMetadataModel],
    result: str
) -> Optional[LLMSociopoliticalOutputModel]: # noqa
    """Validate the output of the LLM."""
    # convert output from YAML to dict
    try:
        breakpoint()
        output_dict = yaml.safe_load(result)
        breakpoint()
        output_model = LLMSociopoliticalOutputModel(**output_dict)
        assert output_model.count == len(posts)
        assert len(output_model.results) == len(posts)
        return output_model
    except Exception as e:
        print(f"Error validating LLM output: {e}")
        return None


def export_validated_llm_output(
    posts: list[RecordClassificationMetadataModel],
    validated_result: LLMSociopoliticalOutputModel,
):
    llm_results = validated_result.results
    label_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H:%M:%S") # noqa
    # TODO: what to do if there's formatting issues? Very likely to happen tbh.
    output_models = [
        SociopoliticalLabelsModel(
            uri=post.uri,
            text=post.text,
            llm_model_name=LLM_MODEL_NAME,
            was_successfully_labeled=True,
            label_timestamp=label_timestamp,
            is_sociopolitical=llm_result.is_sociopolitical,
            political_ideology_label=llm_result.political_ideology_label,
            reason_sociopolitical=llm_result.reason_sociopolitical,
            reason_political_ideology=llm_result.reason_political_ideology,
        )
        for (post, llm_result) in zip(posts, llm_results)
    ]
    batch_insert_sociopolitical_labels(output_models)


async def process_llm_batch(
    post_batch: list[RecordClassificationMetadataModel],
    batch_prompt: str,
    max_retries: Optional[int]=5
) -> dict:
    """Process a batch of posts and prompt using the LLM.

    Retries up to `max_retries` times if the output is invalid. If the output
    is valid, then attempts to write the result.

    Returns a dict based on if the processing failed. If failed, returns the
    post batch and prompt to retry later.
    """
    validated_result = None
    num_retries = 0
    while validated_result is None and num_retries < max_retries:
        result: str = await async_run_query(prompt=batch_prompt)
        validated_result = validated_llm_result(
            posts=post_batch, result=result
        )
        if validated_result is not None:
            print("LLM output validated. Now trying to write to DB.")
            export_validated_llm_output(
                posts=post_batch, validated_result=validated_result
            )
            print("Successfully validated LLM output and results.")
            return {
                "succeeded": True, "response": []
            }
        num_retries += 1
    # TODO: need to think about what happens once we have max retries
    if num_retries >= max_retries:
        print(f"Failed to validate LLM output after maximum {num_retries} retries.") # noqa
    return {
        "succeeded": False,
        "response": (post_batch, batch_prompt)
    }


@track_performance
async def batch_classify_posts(
    posts: list[RecordClassificationMetadataModel],
    batch_size: Optional[int]=DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float]=DEFAULT_DELAY_SECONDS,
    task_name: Optional[str]=DEFAULT_TASK_NAME
):
    """Classify posts in batches."""
    post_batches: list[list[RecordClassificationMetadataModel]] = (
        create_batches(batch_list=posts, batch_size=batch_size)
    )
    failed_batches: list[tuple] = []
    for batch in post_batches:
        batch_prompt: str = generate_batched_post_prompt(
            posts=batch, task_name=task_name
        )
        result_dict = await process_llm_batch(
            post_batch=batch, batch_prompt=batch_prompt
        )
        await asyncio.sleep(seconds_delay_per_batch)
        if not result_dict["succeeded"]:
            failed_batches.append(result_dict["response"])
        breakpoint()

    for (post_batch, batch_prompt) in failed_batches:
        result_dict = process_llm_batch(
            post_batch=post_batch, batch_prompt=batch_prompt
        )
        if not result_dict["succeeded"]:
            # TODO: not sure what to do if it still fails after so many tries?
            print("Failed to process batch after retrying for 2 iterations of batched inference.") # noqa


@track_performance
def run_batch_classification(
    posts: list[RecordClassificationMetadataModel],
    batch_size: Optional[int]=DEFAULT_BATCH_SIZE,
    seconds_delay_per_batch: Optional[float]=DEFAULT_DELAY_SECONDS,
    task_name: Optional[str]=DEFAULT_TASK_NAME
):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        batch_classify_posts(
            posts=posts,
            batch_size=batch_size,
            seconds_delay_per_batch=seconds_delay_per_batch,
            task_name=task_name
        )
    )
