import os
import json
from typing import List, Dict, Any
from faker import Faker
import random
import datetime

DATA_PATH = os.path.join(os.path.dirname(__file__), "sample_posts.json")

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
    "#streamlit",
]


def generate_sample_posts(n: int = 100) -> List[Dict[str, Any]]:
    fake = Faker()
    posts = []
    date_start = datetime.date(2024, 6, 1)
    date_end = datetime.date(2024, 6, 30)
    for i in range(1, n + 1):
        name = fake.user_name()
        user = f"{name}@bsky.social"
        date = fake.date_between_dates(
            date_start=date_start, date_end=date_end
        ).isoformat()
        # Randomly select hashtags for this post
        num_tags = random.choices([0, 1, 2], weights=[0.3, 0.5, 0.2])[0]
        tags = random.sample(HASHTAGS, num_tags) if num_tags > 0 else []
        text = fake.sentence(nb_words=8)
        if tags:
            text += " " + " ".join(tags)
        posts.append(
            {
                "id": i,
                "user": user,
                "text": text,
                "date": date,
                "hashtags": tags,
            }
        )
    return posts


def get_sample_posts() -> List[Dict[str, Any]]:
    if os.path.exists(DATA_PATH):
        with open(DATA_PATH, "r") as f:
            return json.load(f)
    posts = generate_sample_posts(100)
    with open(DATA_PATH, "w") as f:
        json.dump(posts, f)
    return posts
