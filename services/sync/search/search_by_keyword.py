"""Search by keyword."""
from lib.helper import client


def search_by_query(query: str, limit: int = 25):
    """Search by query. Uses the same endpoint as the search bar on the app.
    
    Corresponding lexicon:
    - https://github.com/MarshalX/atproto/blob/3bbfa43c20adf78640abb7ab8cced42fc2e62418/lexicons/app.bsky.feed.searchPosts.json
    
    """
    if limit < 1 or limit > 100:
        raise ValueError("Limit must be between 1 and 100.")


def main():
    pass


if __name__ == "__main__":
    main()
