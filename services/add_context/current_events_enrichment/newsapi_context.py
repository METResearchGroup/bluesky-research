"""Uses the News API in order to get current events.

https://newsapi.org/docs/endpoints/sources
"""
import os
import pickle

from newsapi import NewsApiClient

from lib.helper import NEWSAPI_API_KEY

current_file_directory = os.path.dirname(os.path.abspath(__file__))
news_pickle_fp = os.path.join(current_file_directory, "news_domains.pkl")

newsapi_client = NewsApiClient(api_key=NEWSAPI_API_KEY)


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
    return newsapi_client.get_sources(country="us")


def get_all_news_urls() -> list[str]:
    """Use the News API in order to get a list of US news domains."""
    sources: list[dict] = get_all_news_sources()
    return [source["url"] for source in sources["sources"]]


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
    "nytimes.com", "wsj.com", "washingtonpost.com", "latimes.com",
    "bostonglobe.com", "chicagotribune.com", "seattletimes.com",
    "houstonchronicle.com", "denverpost.com", "dallasnews.com",
    "miamiherald.com", "philly.com", "startribune.com", "ajc.com",
    "mercurynews.com", "sfgate.com", "oregonlive.com", "tampabay.com",
    "azcentral.com", "nj.com", "cleveland.com", "star-telegram.com",
    "post-gazette.com", "charlotteobserver.com", "stltoday.com",
    "kansascity.com", "dispatch.com", "cincinnati.com", "timesunion.com",
    "courant.com", "providencejournal.com", "reviewjournal.com",
    "timesfreepress.com", "timesunion.com", "timesdispatch.com",
    "timespicayune.com", "timesunion.com", "timesunion.com",
]
news_domains = set(api_news_domains + other_news_domains)


def url_is_to_news_domain(url: str) -> bool:
    """Given a URL, check if it is a news domain."""
    return parse_url(url) in news_domains


if __name__ == "__main__":
    export_news_domains()
