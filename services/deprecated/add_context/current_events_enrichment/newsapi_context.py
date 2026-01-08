"""Uses the News API in order to get current events.

https://newsapi.org/docs/endpoints/sources
"""

import os
import pickle

from newsapi import NewsApiClient

from lib.constants import current_datetime
from lib.load_env_vars import EnvVarsContainer
from services.add_context.current_events_enrichment.database import (
    bulk_insert_news_articles,
    bulk_insert_news_outlets,
)
from services.add_context.current_events_enrichment.models import (
    NewsArticleModel,
    NewsOutletModel,
)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
news_pickle_fp = os.path.join(current_file_directory, "news_domains.pkl")

newsapi_client = NewsApiClient(api_key=EnvVarsContainer.get_env_var("NEWSAPI_API_KEY"))

political_party_to_news_outlet_domains_map = {
    "democrat": [
        "abcnews.go.com",
        "msnbc.com",
        "apnews.com",
        "axios.com",
        "bloomberg.com",
        "cbsnews.com",
        "us.cnn.com",
        "politico.com",
        "washingtonpost.com",
        "time.com",
        "usatoday.com",
        "news.vice.com",
        "aljazeera.com",
        "businessinsider.com",
        "news.google.com",
    ],
    "moderate": [
        "wsj.com",
        "newsweek.com",
        "reuters.com",
        "fortune.com",
        "thehill.com",
    ],
    "republican": [
        "theamericanconservative.com",
        "nationalreview.com",
        "washingtontimes.com",
        "foxnews.com",
        "breitbart.com",
    ],
}

political_party_to_news_outlets = {
    "republican": [
        {"domain": "theamericanconservative.com", "id": "the-american-conservative"},
        {"domain": "nationalreview.com", "id": "national-review"},
        {"domain": "washingtontimes.com", "id": "the-washington-times"},
        {"domain": "foxnews.com", "id": "fox-news"},
        {"domain": "breitbart.com", "id": "breitbart-news"},
    ],
    "democrat": [
        {"domain": "abcnews.go.com", "id": "abc-news"},
        {"domain": "msnbc.com", "id": "msnbc"},
        {"domain": "apnews.com", "id": "associated-press"},
        {"domain": "axios.com", "id": "axios"},
        {"domain": "bloomberg.com", "id": "bloomberg"},
        {"domain": "cbsnews.com", "id": "cbs-news"},
        {"domain": "us.cnn.com", "id": "cnn"},
        {"domain": "politico.com", "id": "politico"},
        {"domain": "washingtonpost.com", "id": "the-washington-post"},
        {"domain": "time.com", "id": "time"},
        {"domain": "usatoday.com", "id": "usa-today"},
        {"domain": "news.vice.com", "id": "vice-news"},
        {"domain": "aljazeera.com", "id": "al-jazeera-english"},
        {"domain": "businessinsider.com", "id": "business-insider"},
        {"domain": "news.google.com", "id": "google-news"},
    ],
    "moderate": [
        {"domain": "wsj.com", "id": "the-wall-street-journal"},
        {"domain": "newsweek.com", "id": "newsweek"},
        {"domain": "reuters.com", "id": "reuters"},
        {"domain": "fortune.com", "id": "fortune"},
        {"domain": "thehill.com", "id": "the-hill"},
    ],
}


def parse_domain_from_url(url: str) -> str:
    """Given a URL, parse the domain.

    Example:
    >>> parse_domain_from_url("https://www.nytimes.com")
    "nytimes.com"
    >>> parse_domain_from_url("https://www.foxnews.com")
    "foxnews.com"
    >>> parse_domain_from_url("http://www.foxnews.com")
    "foxnews.com"
    >>> parse_domain_from_url("www.foxnews.com")
    "foxnews.com"
    """
    link = url
    if "https://" in url:
        link = url.replace("https://", "")
    elif "http://" in url:
        link = url.replace("http://", "")
    if "www." in link:
        link = link.replace("www.", "")
    return link


def parse_url(url: str) -> str:
    """Given the URL, parse it. Grab domain plus do postprocessing.

    Get the domain, plus remove anything like trailing subpages.
    """
    parsed_url = parse_domain_from_url(url)
    parsed_url = parsed_url.split("/")[0]
    return parsed_url


def get_all_news_sources() -> list[dict]:
    """Use the News API in order to get a list of US news domains."""
    return newsapi_client.get_sources(country="us")["sources"]


def get_all_news_urls() -> list[str]:
    """Use the News API in order to get a list of US news domains."""
    sources: list[dict] = get_all_news_sources()
    return [source["url"] for source in sources]


def get_all_news_domains() -> list[str]:
    """Use the News API in order to get a list of US news domains."""
    urls: list[str] = get_all_news_urls()
    return [parse_url(url) for url in urls]


def export_news_domains():
    """Exports US news domains as a pkl file."""
    news_domains = get_all_news_domains()
    with open(news_pickle_fp, "wb") as f:
        pickle.dump(news_domains, f)


def load_news_domains():
    """Loads file of news domains and then returns a set."""
    with open(news_pickle_fp, "rb") as f:
        news_domains = pickle.load(f)
    return news_domains


# combining news domains from API plus custom news domains
api_news_domains: list[str] = load_news_domains()
# some of these were likely excluedd due to paywall restrictions plus they
# might've just not been chosen for the API.
other_news_domains = [
    "nytimes.com",
    "wsj.com",
    "washingtonpost.com",
    "latimes.com",
    "bostonglobe.com",
    "chicagotribune.com",
    "seattletimes.com",
    "houstonchronicle.com",
    "denverpost.com",
    "dallasnews.com",
    "miamiherald.com",
    "philly.com",
    "startribune.com",
    "ajc.com",
    "mercurynews.com",
    "sfgate.com",
    "oregonlive.com",
    "tampabay.com",
    "azcentral.com",
    "nj.com",
    "cleveland.com",
    "star-telegram.com",
    "post-gazette.com",
    "charlotteobserver.com",
    "stltoday.com",
    "kansascity.com",
    "dispatch.com",
    "cincinnati.com",
    "timesunion.com",
    "courant.com",
    "providencejournal.com",
    "reviewjournal.com",
    "timesfreepress.com",
    "timesunion.com",
    "timesdispatch.com",
    "timespicayune.com",
    "timesunion.com",
    "timesunion.com",
]
news_domains = set(api_news_domains + other_news_domains)


def url_is_to_news_domain(url: str) -> bool:
    """Given a URL, check if it is a news domain."""
    return parse_url(url) in news_domains


def get_latest_top_headlines_for_source(source_id: str) -> list[dict]:
    """Get the latest top headlines from a news source."""
    articles = newsapi_client.get_top_headlines(sources=source_id)["articles"]
    return [
        {
            "title": article["title"],
            "description": article["description"],
            "url": article["url"],
            "content": article["content"],
            "publishedAt": article["publishedAt"],
        }
        for article in articles
    ]


def get_latest_top_headlines_for_political_party(political_party: str) -> list[dict]:
    """Get the latest top headlines from a news source."""
    news_outlets = political_party_to_news_outlets[political_party]
    return [
        {
            "source": news_outlet["id"],
            "articles": get_latest_top_headlines_for_source(news_outlet["id"]),
        }
        for news_outlet in news_outlets
    ]


def get_latest_articles() -> dict:
    """Get the latest top headlines from all news sources, split by party.

    Takes ~20-30 seconds to run.
    """
    return {
        "republican": get_latest_top_headlines_for_political_party("republican"),  # noqa
        "moderate": get_latest_top_headlines_for_political_party("moderate"),
        "democrat": get_latest_top_headlines_for_political_party("democrat"),
    }


def store_news_outlets_into_db() -> None:
    """Store the news outlets into the database.

    Should be initialized just once unless we add more sources.
    """
    res = []
    for party in political_party_to_news_outlets.keys():
        for news_outlet in political_party_to_news_outlets[party]:
            news_outlet_obj = {
                "outlet_id": news_outlet["id"],
                "domain": news_outlet["domain"],
                "political_party": party,
                "synctimestamp": current_datetime.strftime("%Y-%m-%d-%H:%M:%S"),  # noqa
            }
            NewsOutletModel(**news_outlet_obj)
            res.append(news_outlet_obj)
    bulk_insert_news_outlets(res)
    print(f"Inserted {len(res)} news outlets into the database.")


def organize_articles_by_political_party(latest_articles: dict) -> dict:
    """Returns a dictionary where the key is the political party and the value
    is a list of articles."""
    res = {}
    skipped_articles_count = 0
    print(f"Skipped {skipped_articles_count} articles due to no content.")
    for party in latest_articles.keys():
        res[party] = []
        for news_source_articles_dict in latest_articles[party]:
            news_outlet_source_id = news_source_articles_dict["source"]
            articles = news_source_articles_dict["articles"]
            for article in articles:
                article_obj = {
                    "url": article["url"],
                    "title": article["title"],
                    "content": article["content"],
                    "description": article["description"],
                    "publishedAt": article["publishedAt"],
                    "news_outlet_source_id": news_outlet_source_id,
                    "synctimestamp": current_datetime.strftime("%Y-%m-%d-%H:%M:%S"),  # noqa
                }
                try:
                    NewsArticleModel(**article_obj)
                    res[party].append(article_obj)
                except ValueError as ve:
                    print(
                        f"Skipping article {article_obj['url']} due to no content: {ve}"
                    )  # noqa
                    skipped_articles_count += 1
                except Exception as e:
                    print(f"Error in processing article: {e}")
                    continue
    return res


def store_latest_articles_into_db(latest_articles_by_party: dict) -> None:
    """Store the latest articles into the database.

    Each key is a political party, and each value is a list of articles by news
    orgs with that political lean.
    """
    res: list[dict] = []
    for party in latest_articles_by_party.keys():
        res.extend(latest_articles_by_party[party])
    bulk_insert_news_articles(res)


if __name__ == "__main__":
    export_news_domains()
