"""Generates synthetic data to help us test for intergroup classification.

We want to generate a set of AI-generated posts that we can use to test for
intergroup classification.

We'll use an LLM to generate the posts, and then we'll use the posts to test for
intergroup classification. To simulate diversity, we inject a random persona
(using the Faker library) into the prompt.

We're OK if the quality of the synthetic data is meh, as we're primarily
interested in the scaling properties of the classifier.
"""

import os
import random

from faker import Faker
import pandas as pd
from pydantic import BaseModel

from lib.datetime_utils import generate_current_datetime_str
from services.ml_inference.intergroup.constants import DEFAULT_LLM_MODEL_NAME
from ml_tooling.llm.llm_service import get_llm_service, LLMService
from services.ml_inference.intergroup.prompts import INTERGROUP_EXAMPLES

current_dir = os.path.dirname(os.path.abspath(__file__))
synthetic_data_dir = os.path.join(current_dir, "synthetic_data")
os.makedirs(synthetic_data_dir, exist_ok=True)

class SyntheticIntergroupDataResponseModel(BaseModel):
    text: str

class SyntheticIntergroupDataModel(BaseModel):
    uri: str
    text: str
    gold_label: int

intergroup_generation_prompt = f"""

You are a helpful assistant.

We are interested in posts related to intergroup discussion.

Intergroup discussion is defined as:
- Describes, reports, or implies intergroup discussion

What is NOT intergroup discussion is:
- Is unrelated, speaks only about individuals, is ambiguous, or describes within-group matters.

Your job is to generate a social media post that is {{toggle_generate_intergroup_instruction}}.

You are taking on the following persona: {{persona_description}}.

Below, we have examples of intergroup and not intergroup posts.
- 0 = not intergroup
- 1 = intergroup

{INTERGROUP_EXAMPLES}

Provide your output below.

"""

fake = Faker()

def generate_persona_description() -> str:
    """Generates a random artificial persona description using Faker."""
    occupation = fake.job().lower()
    name = fake.name().lower()
    interests = [interest.lower() for interest in fake.words(nb=3, unique=True)]
    interest_str = ", ".join(interests)
    description = (
        f"{name} is a {occupation} who is interested in {interest_str}"
    )
    return description

def generate_intergroup_prompt(
    is_intergroup: bool
) -> str:
    """Generates a prompt for generating a social media post with the
    given characteristics."""
    toggle_generate_intergroup_instruction = (
        "is related to intergroup discussion"
        if is_intergroup else 
        "is NOT related to intergroup discussion"
    )
    persona_description = generate_persona_description()

    return intergroup_generation_prompt.format(
        toggle_generate_intergroup_instruction=toggle_generate_intergroup_instruction,
        persona_description=persona_description
    )

def generate_random_intergroup_labels(
    num_records: int, prob_intergroup: float
) -> list[int]:
    """Generates a list of random labels for a social media post."""
    return [random.random() < prob_intergroup for _ in range(num_records)]

def generate_batch_prompts(intergroup_labels: list[int]) -> list[str]:
    """Generates a list of prompts for generating a list of social media posts.
    Each prompt is for a social media post that is either intergroup or not intergroup.
    """
    prompts: list[str] = []
    for intergroup_label in intergroup_labels:
        prompt = generate_intergroup_prompt(intergroup_label == 1)
        prompts.append(prompt)
    return prompts

def generate_synthetic_records(
    num_records: int,
    prop_intergroup: float,
    llm_service: LLMService,
) -> list[SyntheticIntergroupDataModel]:
    """Generates a list of synthetic intergroup data records."""
    random_intergroup_labels: list[int] = generate_random_intergroup_labels(num_records, prop_intergroup)
    prompts: list[str] = generate_batch_prompts(random_intergroup_labels)

    print(f"Generated {len(prompts)} prompts")
    print(f"Generating synthetic records for {num_records} prompts...")
    responses: list[SyntheticIntergroupDataResponseModel] = (
        llm_service.structured_batch_completion(
            prompts=prompts,
            response_model=SyntheticIntergroupDataResponseModel,
            model=DEFAULT_LLM_MODEL_NAME,
            role="user",
        )
    )
    print(f"Generated {len(responses)} responses")

    synthetic_records: list[SyntheticIntergroupDataModel] = []
    for i, (response, intergroup_label) in enumerate(zip(responses, random_intergroup_labels)):
        synthetic_record = SyntheticIntergroupDataModel(
            uri=f"at://did:plc:mock{i}",
            text=response.text,
            gold_label=int(intergroup_label)
        )
        synthetic_records.append(synthetic_record)

    return synthetic_records

def export_synthetic_records(
    synthetic_records: list[SyntheticIntergroupDataModel],
):
    """Exports the synthetic records to a CSV file."""
    synthetic_records_df = pd.DataFrame([record.model_dump() for record in synthetic_records])
    export_filename = f"synthetic_records_{generate_current_datetime_str()}.csv"
    export_path = os.path.join(synthetic_data_dir, export_filename)
    synthetic_records_df.to_csv(export_path, index=False)
    print(f"Exported synthetic records to {export_path}")
    return export_path

def main():
    total_records = 20
    prop_intergroup = 0.5
    llm_service: LLMService = get_llm_service()

    synthetic_records: list[SyntheticIntergroupDataModel] = generate_synthetic_records(
        num_records=total_records,
        prop_intergroup=prop_intergroup,
        llm_service=llm_service
    )
    export_synthetic_records(synthetic_records)

if __name__ == "__main__":
    main()
