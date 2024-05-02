"""Loads the latest current events.

Does the following steps:
- Load the latest articles from the NewsAPI client.
- Dumps the articles into the database
- Updates the vector store.
"""
from services.add_context.current_events_enrichment.newsapi_context import (
    get_latest_articles, store_latest_articles_into_db,
    store_news_outlets_into_db
)


def main(
    init_news_outlets: bool = False
) -> None:
    if init_news_outlets:
        store_news_outlets_into_db()
    articles = get_latest_articles()
    store_latest_articles_into_db(articles)


if __name__ == "__main__":
    kwargs = {"init_news_outlets": True}
    main(**kwargs)
