"""Database for storing news articles and news outlets."""
import os
import peewee
from playhouse.shortcuts import model_to_dict
import sqlite3
from typing import Optional

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "news.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class NewsOutlet(BaseModel):
    """News outlet model."""
    outlet_id = peewee.CharField(primary_key=True)
    domain = peewee.CharField()
    political_party = peewee.CharField()
    synctimestamp = peewee.CharField()


class NewsArticle(BaseModel):
    """News article model."""
    url = peewee.CharField(primary_key=True)
    title = peewee.CharField()
    content = peewee.TextField(null=True)
    description = peewee.TextField(null=True)
    publishedAt = peewee.CharField()
    # backref allows us to, for example, use NewsOutlet.articles to get the
    # articles for a given news outlet.
    news_outlet_source_id = peewee.ForeignKeyField(
        NewsOutlet, field='outlet_id', backref='articles'
    )
    synctimestamp = peewee.CharField()


if db.is_closed():
    db.connect()
    db.create_tables([NewsOutlet, NewsArticle])


def create_initial_tables() -> None:
    with db.atomic():
        db.create_tables([NewsOutlet, NewsArticle])


def insert_news_outlet(news_outlet: dict) -> None:
    with db.atomic():
        NewsOutlet.create(**news_outlet)


def bulk_insert_news_outlets(news_outlets: list[dict]) -> None:
    """Bulk insert news outlets, ignoring duplicates."""
    with db.atomic():
        NewsOutlet.insert_many(news_outlets).on_conflict_ignore().execute()


def insert_news_article(news_article: dict) -> None:
    with db.atomic():
        NewsArticle.create(**news_article)


def bulk_insert_news_articles(news_articles: list[dict]) -> None:
    """Bulk insert news articles, ignoring duplicates."""
    total_articles_in_db = NewsArticle.select().count()
    print(f"Total articles in database (before insertion): {total_articles_in_db}")  # noqa
    with db.atomic():
        NewsArticle.insert_many(news_articles).on_conflict_ignore().execute()
    print(f"Inserted {len(news_articles)} articles into the database.")
    total_articles_in_db = NewsArticle.select().count()
    print(f"Total articles in database (after insertion): {total_articles_in_db}")  # noqa


def get_news_outlet_by_id(outlet_id: str) -> Optional[NewsOutlet]:
    try:
        return NewsOutlet.get(NewsOutlet.outlet_id == outlet_id)
    except NewsOutlet.DoesNotExist:
        return None


def get_news_articles_by_outlet_id(outlet_id: str) -> list[NewsArticle]:
    return list(
        NewsArticle.select().where(
            NewsArticle.news_outlet_source_id == outlet_id
        )
    )


def load_articles_from_db() -> dict[str, list[dict]]:
    """Loads all articles from DB. Then, creates a dictionary whose keys
    are the political parties and whose values are the list of articles."""
    articles_by_party = {}
    for news_outlet in NewsOutlet.select():
        articles = get_news_articles_by_outlet_id(news_outlet.outlet_id)
        if news_outlet.political_party not in articles_by_party:
            articles_by_party[news_outlet.political_party] = []
        articles_dicts = [
            model_to_dict(article) for article in articles
        ]
        articles_by_party[news_outlet.political_party].extend(articles_dicts)
    return articles_by_party
