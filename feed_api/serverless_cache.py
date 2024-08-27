"""Helper code for responding to cache requests with managed Redis serverless
cache (via Momento).

Related docs: https://docs.momentohq.com/sdks/python/cache
Console: https://console.gomomento.com/
Cache details: https://console.gomomento.com/cache/data-explorer/bluesky-cache?cp=AWS&region=us-east-2#
"""  # noqa

from datetime import timedelta
import json
from typing import Optional

from momento import CacheClient, Configurations, CredentialProvider
from momento.responses import CacheGet, CacheSet, CreateCache, ListCaches

from lib.aws.secretsmanager import get_secret
from lib.log.logger import get_logger

default_ttl_minutes = 10
default_ttl_seconds = default_ttl_minutes * 60
default_cache_name = "bluesky-cache"

logger = get_logger(__name__)

momento_credentials = json.loads(get_secret("momento_credentials"))
MOMENTO_API_KEY = momento_credentials["MOMENTO_API_KEY"]


def create_client():
    momento_api_key = CredentialProvider.from_string(MOMENTO_API_KEY)
    ttl = timedelta(seconds=float(default_ttl_seconds))
    config = {
        "configuration": Configurations.Laptop.v1(),
        "credential_provider": momento_api_key,
        "default_ttl": ttl,
    }
    return CacheClient.create(**config)


cache_client = create_client()


def create_cache(client, cache_name: str) -> None:
    """Create a cache."""
    resp = client.create_cache(cache_name)
    if isinstance(resp, CreateCache.Success):
        logger.info("Cache created.")
    elif isinstance(resp, CreateCache.Error):
        error = resp
        logger.info(f"Error creating cache: {error.message}")
    else:
        logger.info(f"Unreachable with {resp.message}")


def list_caches(client) -> None:
    """List all caches."""
    logger.info("Listing caches:")
    list_caches_response = client.list_caches()
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
    client,
    cache_name: Optional[str],
    key: str,
    value: str,
    ttl: Optional[timedelta] = None,
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
    response = client.set(cache_name, key, value, ttl)
    if isinstance(response, CacheSet.Success):
        logger.info(f"Successfully set value for key: {key}")
    elif isinstance(response, CacheSet.Error):
        error = response
        logger.info(f"Error setting value: {error.message}")
    else:
        logger.info("Unreachable")


def get(client, cache_name: str, key: str) -> Optional[str]:
    """Get a value from the cache."""
    response = client.get(cache_name, key)
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
    key = "test-key-2"
    value = json.dumps({"foo": "bar"})
    set(
        client=cache_client, cache_name=default_cache_name, key=key, value=value, ttl=60
    )
    res = get(client=cache_client, cache_name=default_cache_name, key=key)
    logger.info(f"{res=}")
