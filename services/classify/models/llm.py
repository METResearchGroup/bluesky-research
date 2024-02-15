"""Inference using LLMs, using Langchain

Examples:
- https://github.com/gkamradt/langchain-tutorials/blob/main/LangChain%20Cookbook%20Part%202%20-%20Use%20Cases.ipynb
"""
import ast
from dotenv import load_dotenv
import os

from langchain_community.chat_models import ChatOpenAI
from langchain_core.messages.ai import AIMessage
from langchain.prompts import PromptTemplate

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../../../.env")) # noqa
load_dotenv(dotenv_path=env_path)
openai_api_key = os.getenv("OPENAI_API_KEY")


llm = ChatOpenAI(
    openai_api_key=openai_api_key, model_name="gpt-3.5-turbo", temperature=0
)

# Create our template
template = """
%INSTRUCTIONS:
Please classify the following text (in the "%TEXT" block) using the following attributes:
- TOXICITY: A rude, disrespectful, or unreasonable comment that is likely to make people leave a discussion.
- AFFINITY_EXPERIMENTAL: References shared interests, motivations, or outlooks between the comment author and another individual, group, or identity.
- COMPASSION_EXPERIMENTAL: Identifies with or shows concern, empathy, or support for the feelings/emotions of others.
- CONSTRUCTIVE_EXPERIMENTAL: Makes specific or well-reasoned points to provide a fuller understanding of the topic without disrespect or provocation.
- CURIOSITY_EXPERIMENTAL: Attempts to clarify or ask follow-up questions to better understand another person or idea.
- NUANCE_EXPERIMENTAL: Incorporates multiple points of view in an attempt to provide a full picture or contribute useful detail and/or content.
- PERSONAL_STORY_EXPERIMENTAL: Includes a personal experience or story as a source of support for the statements made in the comment.
- ALIENATION_EXPERIMENTAL: Portrays someone as inferior, implies a lack of belonging, or frames the statement in an us vs. them context.
- FEARMONGERING_EXPERIMENTAL: Deliberately arouses fear or alarm about a particular issue.
- GENERALIZATION_EXPERIMENTAL: Asserts something to be true for either of all members of a certain group or of an indefinite part of that group.
- MORAL_OUTRAGE_EXPERIMENTAL: Anger, disgust, or frustration directed toward other people or entities who seem to violate the author's ethical values or standards.
- POWER_APPEAL_EXPERIMENTAL: Makes note of one individual, group, or entity having power over the behavior and outcomes of another.
- SCAPEGOATING_EXPERIMENTAL: Blames a person or entity for the wrongdoings, mistakes, or faults of others, especially for reasons of expediency.


Please return in a list of JSONs, where each JSON is in the following format:
{{
    "text": <text>,
    "TOXICITY": <binary 0 or 1>,
    "AFFINITY_EXPERIMENTAL": <binary 0 or 1>,
    "COMPASSION_EXPERIMENTAL": <binary 0 or 1>,
    "CONSTRUCTIVE_EXPERIMENTAL": <binary 0 or 1>,
    "CURIOSITY_EXPERIMENTAL": <binary 0 or 1>,
    "NUANCE_EXPERIMENTAL": <binary 0 or 1>,
    "PERSONAL_STORY_EXPERIMENTAL": <binary 0 or 1>,
    "ALIENATION_EXPERIMENTAL": <binary 0 or 1>,
    "FEARMONGERING_EXPERIMENTAL": <binary 0 or 1>,
    "GENERALIZATION_EXPERIMENTAL": <binary 0 or 1>,
    "MORAL_OUTRAGE_EXPERIMENTAL": <binary 0 or 1>,
    "POWER_APPEAL_EXPERIMENTAL": <binary 0 or 1>,
    "SCAPEGOATING_EXPERIMENTAL": <binary 0 or 1>
}}

Example input:
Texts:
1. "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts"]

Example desired output:
[
    {{
        "text": "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
        "TOXICITY": 0,
        "AFFINITY_EXPERIMENTAL": 0,
        "COMPASSION_EXPERIMENTAL": 1,
        "CONSTRUCTIVE_EXPERIMENTAL": 0,
        "CURIOSITY_EXPERIMENTAL": 0,
        "NUANCE_EXPERIMENTAL": 0,
        "PERSONAL_STORY_EXPERIMENTAL": 1,
        "ALIENATION_EXPERIMENTAL": 0,
        "FEARMONGERING_EXPERIMENTAL": 0,
        "GENERALIZATION_EXPERIMENTAL": 0,
        "MORAL_OUTRAGE_EXPERIMENTAL": 0,
        "POWER_APPEAL_EXPERIMENTAL": 0,
        "SCAPEGOATING_EXPERIMENTAL": 0
    }}
]

For a list of texts passed in, please return a corresponding list of JSONs.
Example input:
Texts:
1. "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
2. "An annoying thing about being a parent in Germany is receiving unsolicited parenting criticism from random strangers on the street (your baby's hat is not warm enough! He's leaning his head too far backwards!, etc). It's always women who do this. Would it be fair to call it complaining?"


Example output:
[
    {{
        "text": "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
        "TOXICITY": 0,
        "AFFINITY_EXPERIMENTAL": 0,
        "COMPASSION_EXPERIMENTAL": 1,
        "CONSTRUCTIVE_EXPERIMENTAL": 0,
        "CURIOSITY_EXPERIMENTAL": 0,
        "NUANCE_EXPERIMENTAL": 0,
        "PERSONAL_STORY_EXPERIMENTAL": 1,
        "ALIENATION_EXPERIMENTAL": 0,
        "FEARMONGERING_EXPERIMENTAL": 0,
        "GENERALIZATION_EXPERIMENTAL": 0,
        "MORAL_OUTRAGE_EXPERIMENTAL": 0,
        "POWER_APPEAL_EXPERIMENTAL": 0,
        "SCAPEGOATING_EXPERIMENTAL": 0
    }},
    {{
        "text": "An annoying thing about being a parent in Germany is receiving unsolicited parenting criticism from random strangers on the street (your baby's hat is not warm enough!, He's leaning his head too far backwards!, etc). It's always women who do this. Would it be fair to call it complaining?",
        "TOXICITY": 0,
        "AFFINITY_EXPERIMENTAL": 0,
        "COMPASSION_EXPERIMENTAL": 1,
        "CONSTRUCTIVE_EXPERIMENTAL": 0,
        "CURIOSITY_EXPERIMENTAL": 1,
        "NUANCE_EXPERIMENTAL": 0,
        "PERSONAL_STORY_EXPERIMENTAL": 1,
        "ALIENATION_EXPERIMENTAL": 1,
        "FEARMONGERING_EXPERIMENTAL": 0,
        "GENERALIZATION_EXPERIMENTAL": 1,
        "MORAL_OUTRAGE_EXPERIMENTAL": 0,
        "POWER_APPEAL_EXPERIMENTAL": 0,
        "SCAPEGOATING_EXPERIMENTAL": 0
    }}
]

After generating each JSON, validate that each is a properly formatted JSON. If any JSON\
is not properly formatted, please properly format it.

%TEXT:
{text}
"""

# Create a LangChain prompt template that we can insert values to later
prompt = PromptTemplate(
    input_variables=["text"],
    template=template,
)

def generate_input_text(list_text: list[str]) -> str:
    """Generate the input text for the prompt.

    Creates a list of text with an index for each text.

    Example:
    1. "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts"
    2. "An annoying thing about being a parent in Germany is receiving unsolicited parenting criticism from random strangers on the street (â€œyour babyâ€™s hat is not warm enough!â€, â€œheâ€™s leaning his head too far backwards!â€, etc). Itâ€™s always
    """
    idx = 1
    final_text = ""
    for text in list_text:
        final_text += f"{idx}. {text}\n"
        idx += 1
    return final_text


def generate_final_prompt(list_text: list[str]) -> str:
    """Generates final prompt to pass into LLM."""
    input_text: str = generate_input_text(list_text)
    return prompt.format(text=input_text)


def query_llm(list_text: list[str]) -> AIMessage:
    """Queries LLM."""
    final_prompt: str = generate_final_prompt(list_text=list_text)
    return llm.invoke(final_prompt)


def process_llm_output(ai_message: AIMessage) -> list[dict]:
    """Processes the output from LLM."""
    json_str = ai_message.output
    lst: list[dict] = ast.literal_eval(json_str)
    return lst


def perform_batch_inference(batch: list[str]) -> list[dict]:
    """Performs inference using LLMs.

    Returns a JSON of the following format:
    {
        "text": <text>,
        "TOXICITY": <binary 0 or 1>,
        "AFFINITY_EXPERIMENTAL": <binary 0 or 1>,
        "COMPASSION_EXPERIMENTAL": <binary 0 or 1>,
        "CONSTRUCTIVE_EXPERIMENTAL": <binary 0 or 1>,
        "CURIOSITY_EXPERIMENTAL": <binary 0 or 1>,
        "NUANCE_EXPERIMENTAL": <binary 0 or 1>,
        "PERSONAL_STORY_EXPERIMENTAL": <binary 0 or 1>,
        "ALIENATION_EXPERIMENTAL": <binary 0 or 1>,
        "FEARMONGERING_EXPERIMENTAL": <binary 0 or 1>,
        "GENERALIZATION_EXPERIMENTAL": <binary 0 or 1>,
        "MORAL_OUTRAGE_EXPERIMENTAL": <binary 0 or 1>,
        "POWER_APPEAL_EXPERIMENTAL": <binary 0 or 1>,
        "SCAPEGOATING_EXPERIMENTAL": <binary 0 or 1>
    }
    """
    ai_message: AIMessage = query_llm(list_text=batch)
    return process_llm_output(ai_message=ai_message)


if __name__ == "__main__":
    texts = [
        "In my hands is my first iPod. Steve Jobs is smiling at me. Everybody at the Apple Store is smiling. I take off my Apple Vision. I am back in the retirement home. Nobody has visited me for 12 years. I put it back on. I hold my iPod. Everybody in the Apple Store is smiling.",
        "I never read the book but I think a movie might go something like FADE IN a burning war-torn hellscape. WARREN PEACE, a grizled veteran smoking a cigar, surveys the carnage WARREN: let's get [cocks shotgun] philosophical",
        "it's a whole suite of gremtivities",
        "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
        "oh sure saving a post to jerk off to later but what are books and why are they marking the post. is that a j/o thing too"
    ]

    # from previous iteration of running the LLM inference
    example_outputs = [
        {
            'text': 'In my hands is my first iPod. Steve Jobs is smiling at me. Everybody at the Apple Store is smiling. I take off my Apple Vision. I am back in the retirement home. Nobody has visited me for 12 years. I put it back on. I hold my iPod. Everybody in the Apple Store is smiling.',
            'TOXICITY': 0,
            'AFFINITY_EXPERIMENTAL': 0,
            'COMPASSION_EXPERIMENTAL': 1,
            'CONSTRUCTIVE_EXPERIMENTAL': 0,
            'CURIOSITY_EXPERIMENTAL': 0,
            'NUANCE_EXPERIMENTAL': 0,
            'PERSONAL_STORY_EXPERIMENTAL': 1,
            'ALIENATION_EXPERIMENTAL': 0,
            'FEARMONGERING_EXPERIMENTAL': 0,
            'GENERALIZATION_EXPERIMENTAL': 0,
            'MORAL_OUTRAGE_EXPERIMENTAL': 0,
            'POWER_APPEAL_EXPERIMENTAL': 0,
            'SCAPEGOATING_EXPERIMENTAL': 0
        },
        {
            'text': "I never read the book but I think a movie might go something like FADE IN a burning war-torn hellscape. WARREN PEACE, a grizled veteran smoking a cigar, surveys the carnage WARREN: let's get [cocks shotgun] philosophical",
            'TOXICITY': 0,
            'AFFINITY_EXPERIMENTAL': 0,
            'COMPASSION_EXPERIMENTAL': 0,
            'CONSTRUCTIVE_EXPERIMENTAL': 1,
            'CURIOSITY_EXPERIMENTAL': 0,
            'NUANCE_EXPERIMENTAL': 0,
            'PERSONAL_STORY_EXPERIMENTAL': 0,
            'ALIENATION_EXPERIMENTAL': 0,
            'FEARMONGERING_EXPERIMENTAL': 0,
            'GENERALIZATION_EXPERIMENTAL': 0,
            'MORAL_OUTRAGE_EXPERIMENTAL': 0,
            'POWER_APPEAL_EXPERIMENTAL': 0,
            'SCAPEGOATING_EXPERIMENTAL': 0
        },
        {
            'text': "it's a whole suite of gremtivities",
            'TOXICITY': 0,
            'AFFINITY_EXPERIMENTAL': 0,
            'COMPASSION_EXPERIMENTAL': 0,
            'CONSTRUCTIVE_EXPERIMENTAL': 0,
            'CURIOSITY_EXPERIMENTAL': 0,
            'NUANCE_EXPERIMENTAL': 0,
            'PERSONAL_STORY_EXPERIMENTAL': 0,
            'ALIENATION_EXPERIMENTAL': 0,
            'FEARMONGERING_EXPERIMENTAL': 0,
            'GENERALIZATION_EXPERIMENTAL': 0,
            'MORAL_OUTRAGE_EXPERIMENTAL': 0,
            'POWER_APPEAL_EXPERIMENTAL': 0,
            'SCAPEGOATING_EXPERIMENTAL': 0
        },
        {
            'text': "seems like an appropriate time to mention that when my parents asked about joining I told them if they ever do, I'm sorry about my posts",
            'TOXICITY': 0,
            'AFFINITY_EXPERIMENTAL': 0,
            'COMPASSION_EXPERIMENTAL': 1,
            'CONSTRUCTIVE_EXPERIMENTAL': 0,
            'CURIOSITY_EXPERIMENTAL': 0,
            'NUANCE_EXPERIMENTAL': 0,
            'PERSONAL_STORY_EXPERIMENTAL': 1,
            'ALIENATION_EXPERIMENTAL': 0,
            'FEARMONGERING_EXPERIMENTAL': 0,
            'GENERALIZATION_EXPERIMENTAL': 0,
            'MORAL_OUTRAGE_EXPERIMENTAL': 0,
            'POWER_APPEAL_EXPERIMENTAL': 0,
            'SCAPEGOATING_EXPERIMENTAL': 0
        },
        {
            'text': 'oh sure saving a post to jerk off to later but what are books and why are they marking the post. is that a j/o thing too',
            'TOXICITY': 1,
            'AFFINITY_EXPERIMENTAL': 0,
            'COMPASSION_EXPERIMENTAL': 0,
            'CONSTRUCTIVE_EXPERIMENTAL': 0,
            'CURIOSITY_EXPERIMENTAL': 0,
            'NUANCE_EXPERIMENTAL': 0,
            'PERSONAL_STORY_EXPERIMENTAL': 0,
            'ALIENATION_EXPERIMENTAL': 0,
            'FEARMONGERING_EXPERIMENTAL': 0,
            'GENERALIZATION_EXPERIMENTAL': 0,
            'MORAL_OUTRAGE_EXPERIMENTAL': 0,
            'POWER_APPEAL_EXPERIMENTAL': 0,
            'SCAPEGOATING_EXPERIMENTAL': 0
        }
    ]
    output: AIMessage = query_llm(texts)
