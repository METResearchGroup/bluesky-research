"""Authenticating users for the feed API.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/auth.py
"""  # noqa

from atproto import DidInMemoryCache, IdResolver, verify_jwt
from atproto.exceptions import TokenInvalidSignatureError
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from lib.log.logger import get_logger

_CACHE = DidInMemoryCache()
_ID_RESOLVER = IdResolver(cache=_CACHE)
_AUTHORIZATION_HEADER_VALUE_PREFIX = "Bearer "

logger = get_logger(__name__)


class AuthorizationError(Exception):
    pass


security = HTTPBearer()


async def validate_auth(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> str:
    """Validate authorization header.

    Args:
        credentials: The authorization credentials.

    Returns:
        str: Requester DID.

    Raises:
        HTTPException: If the authorization header is invalid.
    """
    # no need to check for "Bearer " prefix as in the docs
    # at https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/auth.py#L17
    # since FastAPI strips "Bearer " prefix from the header
    jwt = credentials.credentials
    try:
        did = verify_jwt(jwt, _ID_RESOLVER.did.resolve_atproto_key).iss
        logger.info(f"Validated request for DID={did}...")
        return did
    except TokenInvalidSignatureError as e:
        logger.error(f"Invalid token signature: {e}")
        raise HTTPException(status_code=403, detail="Invalid signature") from e
    except Exception as e:
        logger.error(f"Token validation failed: {e}")
        raise HTTPException(status_code=403, detail="Token validation failed")
