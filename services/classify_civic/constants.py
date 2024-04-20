import os

current_directory = os.path.dirname(os.path.abspath(__file__))
NYTIMES_DB_NAME = "nytimes_articles.db"
NYTIMES_ARTICLE_TABLE_NAME = "articles"
SQLITE_TABLE_PATH = os.path.join(
    current_directory, "update_current_events", "nytimes_articles.db"
)
VECTOR_DB_NAME = "faiss_index_nytimes"
FAISS_INDEX_PATH = os.path.join(
    current_directory, "update_current_events", VECTOR_DB_NAME
)
