"""Sets up test SQLite DB for the dump-db-to-parquet service."""
pass


def setup_new_db() -> None:
    pass


def create_mock_firehose_posts() -> list[dict]:
    pass


def insert_mock_firehose_posts(posts: list[dict]) -> None:
    pass


def main() -> None:
    setup_new_db()
    posts: list[dict] = create_mock_firehose_posts()
    insert_mock_firehose_posts(posts)


if __name__ == "__main__":
    main()
