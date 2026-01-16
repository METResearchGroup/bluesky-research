from services.backfill.models import EnqueueServicePayload


class EnqueueService:
    def __init__(self):
        self.enqueue_posts_service = EnqueuePostsService()
        self.enqueue_posts_used_in_feeds_service = EnqueuePostsUsedInFeedsService()

    def enqueue_records(self, payload: EnqueueServicePayload):
        if payload.record_type == "posts":
            self._enqueue_posts(payload)
        elif payload.record_type == "posts_used_in_feeds":
            self._enqueue_posts_used_in_feeds(payload)
        else:
            raise ValueError(f"Invalid record type: {payload.record_type}")

    def _enqueue_posts(self, payload: EnqueueServicePayload):
        self.enqueue_posts_service.enqueue_records(payload)

    def _enqueue_posts_used_in_feeds(self, payload: EnqueueServicePayload):
        self.enqueue_posts_used_in_feeds_service.enqueue_records(payload)


class EnqueuePostsService:
    def enqueue_records(self, payload: EnqueueServicePayload):
        pass


class EnqueuePostsUsedInFeedsService:
    def enqueue_records(self, payload: EnqueueServicePayload):
        pass
