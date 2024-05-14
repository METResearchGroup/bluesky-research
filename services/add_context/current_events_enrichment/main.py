"""Loads the latest current events.

Does the following steps:
- Load the latest articles from the NewsAPI client.
- Dumps the articles into the database
- Updates the vector store.
"""
from services.add_context.current_events_enrichment.database import (
    load_articles_from_db
)
from services.add_context.current_events_enrichment.newsapi_context import (
    get_latest_articles, store_latest_articles_into_db,
    store_news_outlets_into_db, organize_articles_by_political_party
)
from services.add_context.current_events_enrichment.vector_store import (
    update_vector_store
)


def main(
    init_news_outlets: bool = False,
    update_vector_store_only: bool = False,
    load_articles_from_db_bool: bool = False
) -> None:
    if load_articles_from_db_bool:
        latest_articles_by_party: dict = load_articles_from_db()
        print(f"Loaded {len(latest_articles_by_party)} articles from the database.")  # noqa
    else:
        if init_news_outlets:
            store_news_outlets_into_db()
        articles: dict = get_latest_articles()
        latest_articles_by_party: dict = organize_articles_by_political_party(articles)  # noqa
    if not update_vector_store_only:
        store_latest_articles_into_db(latest_articles_by_party)
    update_vector_store(latest_articles_by_party)
    print(f"Finished syncing the latest articles and adding to DB and vector store.")  # noqa


if __name__ == "__main__":
    kwargs = {
        "init_news_outlets": False,
        "update_vector_store_only": False,
        "load_articles_from_db_bool": True
    }
    main(**kwargs)
