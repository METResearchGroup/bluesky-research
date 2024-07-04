"""Test script 3.

This script tests if we can run inference with the LLM.

Assumes that we can load the data.
"""
from lib.db.sql.preprocessing_database import get_filtered_posts
from services.preprocess_raw_data.models import FilteredPreprocessedPostModel

MIN_TEXT_LENGTH = 10

llm_dir = "/kellogg/data/llm_models_opensource/llama3_meta_huggingface"
llm_model = "meta-llama/Llama-2-7b-chat-hf"  # can try 70b model next


def generate_query(texts: list[str]) -> str:
    enumerated_texts = [
        f"{idx+1}. {text}" for (idx, text) in enumerate(texts)
    ]
    enumerated_texts_str = '\n'.join(enumerated_texts)
    return f"""
        [INST]
        
        Pretend that you are a sociopolitical classifier, tasked with identifying if a piece of text
        contains content related to US politics or US social issues.

        You will be given a series of enumerated texts. Please classify if the given texts
        are sociopolitical or not.

        Return a JSON, with keys corresponding to the text number.
        The value should be a JSON with two fields:
        - Label: Binary values (1 or 0)
        - Reason: a 1-sentence reason for the classification.

        Return only the output JSON, and verify that it is valid JSON.

        Here is an example list of texts:

        <TEXTS>
        1. I hate that the president is so lenient on crimes.
        2. Go Chiefs! Hope they capture the Super Bowl this year.
        3. I just drew this self-portrait, let me know what you think.
        4. A vote for Trump is a vote for hatred.

        Here is an example response:
        {
            "1": {"Label": 1, "Reason": "<reason provided by LLM>"},
            "2": {"Label": 0, "Reason": "<reason provided by LLM>"},
            "3": {"Label": 0, "Reason": "<reason provided by LLM>"},
            "4": {"Label": 1, "Reason": "<reason provided by LLM>"}
        }
        
        Below are the list of texts for you to classify:
        <TEXTS>
        {enumerated_texts_str}
        [/INST]
    """


if __name__ == "__main__":
    posts: list[FilteredPreprocessedPostModel] = get_filtered_posts()
    subset_posts = posts[:5]
    texts = [
        post.record.text for post in subset_posts
        if len(posts) >= MIN_TEXT_LENGTH
    ]
    print(f"Number of filtered posts: {len(posts)}")
    print("Posts successfully loaded!")
