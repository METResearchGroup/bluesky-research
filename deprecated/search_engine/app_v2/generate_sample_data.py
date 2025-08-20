import os
import json
from typing import List, Dict, Any
import random

from pydantic import BaseModel, Field
from ml_tooling.llm.inference import run_query

DATA_PATH = os.path.join(os.path.dirname(__file__), "sample_posts.json")

GENERATE_TEXT_PROMPT = """

You are creating social media posts. Imagine that I give you posts with the following
parameters. Create an example social media post, Twitter-style, that matches the parameters.

Parameters:
- Valence: {valence}
- Toxic: {toxic}
- Political: {political}
- Political Slant: {slant}
- Hashtags: {hashtags}
"""

HASHTAGS = [
    "#climate",
    "#election",
    "#sports",
    "#ai",
    "#music",
    "#news",
    "#fun",
    "#bsky",
    "#python",
    "#trump",
    "#politics",
]

SAMPLE_POSTS: List[Dict[str, Any]] = []
valences = ["positive", "neutral", "negative"]
slants = ["left", "center", "right", "unclear"]
users = [
    "alice",
    "bob",
    "carol",
    "dave",
    "eve",
    "frank",
    "grace",
    "heidi",
    "ivan",
    "judy",
]
users = [f"{user}@bsky.social" for user in users]
dates = [f"2024-06-{str(i+1).zfill(2)}" for i in range(10)]
hashtags_list = [
    ["#climate"],
    ["#election"],
    ["#climate"],
    [],
    ["#climate", "#election"],
    [],
    ["#ai"],
    ["#music"],
    ["#news"],
    ["#fun"],
]


def generate_prompt_for_text_generation(
    valence: str, toxic: bool, political: bool, slant: str, hashtags: List[str]
) -> str:
    prompt = GENERATE_TEXT_PROMPT.format(
        valence=valence,
        toxic=toxic,
        political=political,
        slant=slant,
        hashtags=hashtags,
    )
    return prompt


class TextGenerationResponseModel(BaseModel):
    text: str = Field(..., description="The generated text.")


def generate_new_posts():
    def generate_text(prompt: str) -> str:
        custom_kwargs = {
            "temperature": 0.0,
            "response_format": TextGenerationResponseModel,
        }
        try:
            response = run_query(
                prompt=prompt,
                role="user",
                model_name="GPT-4o mini",
                num_retries=2,
                custom_kwargs=custom_kwargs,
            )
            # Try to parse the response as JSON
            result = json.loads(response)
            return result["text"]
        except Exception as e:
            print(f"Unable to generate response: {e}")
        return response

    try:
        default_number_of_posts = 100
        for i in range(default_number_of_posts):
            if i % 10 == 0:
                print(f"Generating post {i+1} of {default_number_of_posts}")
            valence = valences[i % len(valences)]
            toxic = None
            if valence == "negative":
                toxic = random.choice([True, False])
            political = i % 2 == 0
            slant = slants[i % len(slants)] if political else None
            hashtags = hashtags_list[i % len(hashtags_list)]
            user = users[i % len(users)]
            date = dates[i % len(dates)]
            generate_text_prompt = generate_prompt_for_text_generation(
                valence=valence,
                toxic=toxic,
                political=political,
                slant=slant,
                hashtags=hashtags,
            )
            text = generate_text(generate_text_prompt)
            SAMPLE_POSTS.append(
                {
                    "id": i + 1,
                    "user": user,
                    "text": text,
                    "date": date,
                    "hashtags": hashtags,
                    "valence": valence,
                    "toxic": toxic,
                    "political": political,
                    "slant": slant,
                }
            )

        with open(DATA_PATH, "w") as f:
            json.dump(SAMPLE_POSTS, f, indent=2)
    except Exception as e:
        print(f"Error generating posts: {e}")
        import traceback

        traceback.print_exc()
        breakpoint()


def get_sample_posts() -> List[Dict[str, Any]]:
    if not os.path.exists(DATA_PATH):
        generate_new_posts()
    with open(DATA_PATH, "r") as f:
        SAMPLE_POSTS = json.load(f)
    return SAMPLE_POSTS


if __name__ == "__main__":
    generate_new_posts()
