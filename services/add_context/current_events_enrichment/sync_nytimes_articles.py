"""Syncs NYTimes articles with the database.

Can run once a day to update the database with information, or possibly
once every 12 hours.

Rate limit of 5 queries/min (https://developer.nytimes.com/faq)
"""
import requests
import time
from typing import Optional

from lib.constants import current_datetime
from lib.helper import NYTIMES_API_KEY, track_function_runtime
from services.add_context.current_events_enrichment.database import batch_write_articles # noqa


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
        time.sleep(20) # wait 20 seconds to avoid rate limiting
        print(f"Finished loading {res['num_results']} articles.")
        print('-' * 10)
    return top_stories


def process_article_facets(article: dict) -> str:
    """Processes article facets and returns a string.
    
    String is separated on ; as this is a much less common separator than , and
    some text use ',' in their text.

    Example:
    {
        'des_facet': ['New York State Civil Case Against Trump (452564/2022)'],
        'org_facet': [],
        'per_facet': ['Trump, Donald J', 'James, Letitia'],
        'geo_facet': ['New York State']
    }
    """
    facets = article["des_facet"] + article["org_facet"] + article["per_facet"] + article["geo_facet"]
    return ';'.join(facets)


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
    facets = process_article_facets(article)

    return {
        "nytimes_uri": nytimes_uri,
        "title": title,
        "abstract": abstract,
        "url": url,
        "published_date": published_date,
        "captions": lst_captions,
        "facets": facets,
        "synctimestamp": current_datetime.strftime("%Y-%m-%d-%H:%M:%S"),
        "sync_date": current_datetime.strftime("%Y-%m-%d")
    }


def process_articles(articles: list) -> list:
    """Processes a list of articles."""
    return [process_article(article) for article in articles]


def write_articles_to_db(articles: list[dict]) -> None:
    """Writes articles to SQLite DB."""
    num_articles = len(articles)
    print(f"Writing {num_articles} articles to DB.")
    batch_write_articles(articles=articles)
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


if __name__ == "__main__":
    sync_latest_nytimes_topstories()
