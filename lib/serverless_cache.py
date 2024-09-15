"""Helper code for responding to cache requests with managed Redis serverless
cache (via Momento).

Related docs: https://docs.momentohq.com/sdks/python/cache
Console: https://console.gomomento.com/
Cache details: https://console.gomomento.com/cache/data-explorer/bluesky-cache?cp=AWS&region=us-east-2#
"""  # noqa

from datetime import timedelta
import json
from typing import Optional, Union

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

from lib.aws.secretsmanager import get_secret
from lib.log.logger import get_logger

default_ttl_minutes = 10
default_ttl_seconds = default_ttl_minutes * 60
# 18 hours default long-lived TTL.
# for feeds, have feeds live for a while in cache.
default_long_lived_ttl_hours = 18
default_long_lived_ttl_seconds = default_long_lived_ttl_hours * 60 * 60
default_cache_name = "bluesky-cache"

logger = get_logger(__name__)

momento_credentials = json.loads(get_secret("momento_credentials"))
MOMENTO_API_KEY = momento_credentials["MOMENTO_API_KEY"]


class ServerlessCache:
    def __init__(self):
        self.momento_api_key = CredentialProvider.from_string(MOMENTO_API_KEY)
        self.ttl = timedelta(seconds=float(default_ttl_seconds))
        self.client = self.create_client()

    def create_client(self):
        config = {
            "configuration": Configurations.Laptop.v1(),
            "credential_provider": self.momento_api_key,
            "default_ttl": self.ttl,
        }
        return CacheClient.create(**config)

    def create_cache(self, cache_name: str) -> None:
        """Create a cache."""
        resp = self.client.create_cache(cache_name)
        if isinstance(resp, CreateCache.Success):
            logger.info("Cache created.")
        elif isinstance(resp, CreateCache.Error):
            error = resp
            logger.info(f"Error creating cache: {error.message}")
        else:
            logger.info(f"Unreachable with {resp.message}")

    def list_caches(self) -> None:
        """List all caches."""
        logger.info("Listing caches:")
        list_caches_response = self.client.list_caches()
        if isinstance(list_caches_response, ListCaches.Success):
            success = list_caches_response
            for cache_info in success.caches:
                logger.info(f"- {cache_info.name!r}")
        elif isinstance(list_caches_response, ListCaches.Error):
            error = list_caches_response
            logger.info(f"Error listing caches: {error.message}")
        else:
            logger.info("Unreachable")

    def set(
        self,
        cache_name: Optional[str],
        key: str,
        value: str,
        ttl: Union[Optional[timedelta], int] = None,
    ):
        if cache_name is None:
            logger.info(f"Using default cache name: {default_cache_name}")
            cache_name = default_cache_name
        if ttl is None:
            logger.info(f"Using default ttl: {default_ttl_seconds} seconds")
            ttl = default_ttl_seconds
        if not isinstance(ttl, timedelta):
            ttl = timedelta(seconds=float(ttl))
        if not isinstance(value, str):
            raise ValueError(f"Value must be a string, got {type(value)}")
        # NOTE: all values will be stored as string.
        response = self.client.set(cache_name, key, value, ttl)
        if isinstance(response, CacheSet.Success):
            logger.info(f"Successfully set value in cache {cache_name} for key: {key}")
        elif isinstance(response, CacheSet.Error):
            error = response
            logger.info(f"Error setting value: {error.message}")
        else:
            logger.info("Unreachable")

    def get(self, cache_name: str, key: str) -> Optional[str]:
        """Get a value from the cache."""
        response = self.client.get(cache_name, key)
        if isinstance(response, CacheGet.Hit):
            # NOTE: all responses will be returned as string.
            return response.value_string
        elif isinstance(response, CacheGet.Miss):
            logger.info(f"Cache miss for key: {key}")
            return None
        else:
            logger.info(f"Unreachable with {response.message}")
            return None


if __name__ == "__main__":
    cache = ServerlessCache()  # Instantiate the ServerlessCache class
    key = "test-key-2"
    value = json.dumps({"foo": "bar"})
    cache.set(
        cache_name=default_cache_name, key=key, value=value, ttl=60
    )  # Use the set method of ServerlessCache
    res = cache.get(
        cache_name=default_cache_name, key=key
    )  # Use the get method of ServerlessCache
    logger.info(f"{res=}")
