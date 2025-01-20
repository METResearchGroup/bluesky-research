from services.backfill.posts.main import backfill_posts


def backfill_records(payload: dict):
    record_type = payload.get("record_type")
    if record_type == "posts":
        backfill_posts(payload)
    pass
