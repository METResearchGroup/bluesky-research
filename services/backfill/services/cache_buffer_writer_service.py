class CacheBufferWriterService:
    """Manages writing cache buffers to the databases.

    We decouple writing the cache to permanent storage from
    clearing the cache. However, in our setup, we do the writes
    first and then clear the cache. This assumes that whatever
    was in the cache at the time of running clear_cache was already
    written to permanent storage. This is a known limitation and
    assumption that works for our use case. But it can fail for cases
    like multiple concurrent runs all trying to write the cache to
    permanent storage and then clear it. For our use case where this
    is run as a single-threaded application, this is OK.
    """

    def __init__(self):
        # TODO: delegate to a queue service AND to the repo.
        # TODO: update BackfillDataRepository and the adapters
        # to do the writing component.
        pass

    def write_cache(self, service: str):
        pass

    def clear_cache(self, service: str):
        pass
