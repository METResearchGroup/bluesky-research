"""DB functionalities for civic classification."""
import sqlite3

from services.classify_civic.constants import (
    NYTIMES_DB_NAME, NYTIMES_ARTICLE_TABLE_NAME
)

conn = sqlite3.connect(NYTIMES_DB_NAME)
cursor = conn.cursor()


def create_db_and_table(table_name: str = NYTIMES_ARTICLE_TABLE_NAME) -> None:
    """Create DB and table."""
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            nytimes_uri TEXT,
            title TEXT,
            abstract TEXT,
            url TEXT,
            published_date TEXT,
            captions TEXT
        )
    ''')
    conn.commit()
    conn.close()
