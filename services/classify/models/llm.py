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

from services.classify.models.helper import template

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../../../.env")) # noqa
load_dotenv(dotenv_path=env_path)
openai_api_key = os.getenv("OPENAI_API_KEY")


llm = ChatOpenAI(
    openai_api_key=openai_api_key, model_name="gpt-3.5-turbo", temperature=0
)

# Create our template

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
    json_str = ai_message.content
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

    # NOTE: (1) follower cutoff for "big accounts" as proxy for valid accounts, 
    # (2) NGOs, (3) direct quotes, can be criteria for managing separately
    

    # NOTE: think about cases that have a strong moral stance but not considered toxic

    texts = [
        "Restricting abortion access in all cases is morally wrong.",
        "The situation in Gaza is a horrible humanitarian crisis and we shouldn't ignore it.",
        "I can't believe the refs are so biased in the Super Bowl!",
        "I think the gaza situation is a horrible humanitarian crisis and if you disagree you are an awful person"
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
    outputs = process_llm_output(output)

    # NOTE: won't just pick up keywords, it picks up context
    # NOTE: ask GPT to produce examples like these.
    breakpoint()
