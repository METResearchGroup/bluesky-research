"""DB functionalities for civic classification."""
import peewee
import os
import sqlite3

import pandas as pd

current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "nytimes_articles.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)
DEFAULT_BATCH_WRITE_SIZE = 100

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()

class BaseModel(peewee.Model):
    class Meta:
        database = db


class NYTimesArticle(BaseModel):
    """NYTimes Article."""
    id = peewee.AutoField()
    nytimes_uri = peewee.CharField(unique=True)
    title = peewee.CharField()
    abstract = peewee.TextField()
    url = peewee.TextField()
    published_date = peewee.CharField()
    captions = peewee.TextField()
    facets = peewee.TextField()
    # deprecated (use synctimestamp)
    sync_timestamp = peewee.DateTimeField(default=peewee.fn.now())
    synctimestamp = peewee.CharField()
    sync_date = peewee.DateField(default=peewee.fn.now())


def create_initial_articles_table() -> None:
    """Create the initial articles table."""
    with db.atomic():
        db.create_tables([NYTimesArticle])


def batch_write_articles(articles: list[dict]) -> None:
    """Batch create articles in chunks.
    
    Uses peewee's chunking functionality to write articles in chunks.
    """
    with db.atomic():
        for idx in range(0, len(articles), DEFAULT_BATCH_WRITE_SIZE):
            # Ignore insert for rows that would cause a unique constraint violation
            NYTimesArticle.insert_many(
                articles[idx:idx + DEFAULT_BATCH_WRITE_SIZE]
            ).on_conflict_ignore().execute()
    print(f"Batch created {len(articles)} articles.")


def get_all_articles() -> list[NYTimesArticle]:
    """Get all articles from the database."""
    return list(NYTimesArticle.select())


def get_all_articles_as_list_dicts() -> list[dict]:
    """Get all articles from the database as a list of dictionaries."""
    return [article.__dict__['__data__'] for article in get_all_articles()]


def get_all_articles_as_df() -> pd.DataFrame:
    """Get all articles from the database as a pandas DataFrame."""
    return pd.DataFrame(get_all_articles_as_list_dicts())


if __name__ == "__main__":
    #create_initial_articles_table()
    #print("Created initial articles table.")
    #num_articles = len(get_all_articles())
    #print(f"Total number of articles: {num_articles}")
    #print("Finished running the script.")
    # how to add a new column to a table (here, 'synctimestamp')
    #from lib.db.sql.helper import add_new_column_to_table
    #add_new_column_to_table(cls=NYTimesArticle, cursor=cursor, db=db, colname="synctimestamp")
    pass
