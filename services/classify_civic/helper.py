import os

current_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_TABLE_PATH = os.path.join(
    current_directory,"update_current_events", "nytimes_articles.db"
)
