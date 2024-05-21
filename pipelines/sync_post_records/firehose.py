from services.sync.stream.app import start_app


def get_posts() -> None:
    print("Getting posts from the firehose.")
    start_app()
