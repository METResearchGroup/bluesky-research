"""Syncs NYTimes articles with the database.

Can run once a day to update the database with information, or possibly
once every 12 hours.

Rate limit of 5 queries/min (https://developer.nytimes.com/faq)
"""
import requests
from typing import Optional

import pandas as pd

from lib.constants import current_datetime
from lib.helper import add_rate_limit, NYTIMES_API_KEY, track_function_runtime
from services.classify_civic.update_current_events.db import conn, cursor


SECTION_TOPSTORIES = ["arts", "automobiles", "books/review", "business", 
    "fashion", "food", "health", "home", "insider", "magazine", "movies",
    "nyregion", "obituaries", "opinion", "politics", "realestate", "science",
    "sports", "sundayreview", "technology", "theater", "t-magazine", "travel",
    "upshot", "us", "world"
]

# probably the most relevant ones?
DEFAULT_SECTIONS = ["home", "business", "politics", "us", "world", "technology"] # noqa

def generate_request_url(endpoint: str) -> str:
    """Generates the request URL."""
    return f"https://api.nytimes.com/svc/{endpoint}?api-key={NYTIMES_API_KEY}"


# rate limit of 5 queries/min
def load_section_topstories(section: str) -> dict:
    """Loads the top stories from a section."""
    if not section in SECTION_TOPSTORIES:
        raise ValueError(f"Section {section} is not a valid section.")
    endpoint = f"topstories/v2/{section}.json"
    url = generate_request_url(endpoint)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def load_all_section_topstories(sections: Optional[list] = DEFAULT_SECTIONS) -> dict:
    """Loads all the top stories from a list of sections.
    
    Returns a dictionary with the following:
        keys:
            :section (str): the section that the articles relate to
        values:
            :section (list[dict]): list of article results.
    """
    top_stories = {}
    for section in sections:
        print(f"Loading articles for {section} section...")
        res = load_section_topstories(section)
        top_stories[section] = res["results"]
        print(f"Finished loading {res['num_results']} articles.")
        print('-' * 10)
    return top_stories


def process_article(article: dict) -> dict:
    """Processes an article.

    We only need a subset of the information returned about each article. We
    want to grab the least information possible that will tell us what an
    article is about.
    """
    nytimes_uri = article["uri"]
    title = article["title"]
    abstract = article["abstract"]
    url = article["url"]
    published_date = article["published_date"]
    set_captions = set()
    for multimedia in article["multimedia"]:
        set_captions.add(multimedia["caption"])
    lst_captions = list(set_captions)

    return {
        "nytimes_uri": nytimes_uri,
        "title": title,
        "abstract": abstract,
        "url": url,
        "published_date": published_date,
        "captions": lst_captions
    }


def process_articles(articles: list) -> list:
    """Processes a list of articles."""
    return [process_article(article) for article in articles]


def write_article_to_db(article: dict) -> None:
    """Writes an article to the database."""
    captions_str = ", ".join(article["captions"])
    cursor.execute('''
        INSERT INTO articles (nytimes_uri, title, abstract, url, published_date, captions)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (article["nytimes_uri"], article['title'], article['abstract'], article['url'], article['published_date'], captions_str) # noqa
    )
    conn.commit()


def write_articles_to_db(articles: list) -> None:
    """Writes articles to SQLite DB."""
    num_articles = len(articles)
    print(f"Writing {num_articles} articles to DB.")
    for article in articles:
        write_article_to_db(article)
    print(f"Finished writing {num_articles} articles to DB.")


@track_function_runtime
def sync_latest_nytimes_topstories(sections: Optional[list] = DEFAULT_SECTIONS) -> None:
    """Syncs the latest NYTimes top stories."""
    print(f"Syncing latest NYTimes stories at {current_datetime}")
    section_to_topstories_map: dict[str, list] = (
        load_all_section_topstories(sections)
    )
    for section, topstories_list in section_to_topstories_map.items():
        print(f"Writing the latest articles for the {section} section to DB.")
        processed_articles = process_articles(topstories_list)
        write_articles_to_db(processed_articles)
        print('-' * 10)
    print(f"Finished writing the latest topstories ")


@track_function_runtime
def load_all_articles_as_df() -> pd.DataFrame:
    """Loads all existing articles as a Pandas DataFrame."""
    query = "SELECT * FROM articles"
    df_articles = pd.read_sql_query(query, conn)
    return df_articles


def vectorize_articles(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorizes the articles."""
    pass


if __name__ == "__main__":
    sync_latest_nytimes_topstories()
