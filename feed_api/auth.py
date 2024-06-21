"""Authenticating users for the feed API.

Based on https://github.com/MarshalX/bluesky-feed-generator/blob/main/server/auth.py
"""  # noqa
from atproto import DidInMemoryCache, IdResolver, verify_jwt
from atproto.exceptions import TokenInvalidSignatureError
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from feed_api.helper import get_valid_dids

_CACHE = DidInMemoryCache()
_ID_RESOLVER = IdResolver(cache=_CACHE)
_AUTHORIZATION_HEADER_VALUE_PREFIX = 'Bearer '


valid_dids = get_valid_dids()


class AuthorizationError(Exception):
    pass


# Dependency to be used in FastAPI routes
security = HTTPBearer()


def get_requester_did(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:  # noqa
    """FastAPI dependency to validate authorization header and extract DID."""
    auth_header = credentials.credentials
    if not auth_header.startswith(_AUTHORIZATION_HEADER_VALUE_PREFIX):
        raise HTTPException(status_code=401, detail="Invalid authorization header")  # noqa

    jwt = auth_header[len(_AUTHORIZATION_HEADER_VALUE_PREFIX):].strip()

    try:
        return verify_jwt(jwt, _ID_RESOLVER.did.resolve_atproto_key).iss
    except TokenInvalidSignatureError:
        raise HTTPException(status_code=401, detail="Invalid signature")
    except AuthorizationError as e:
        raise HTTPException(status_code=401, detail=str(e))


def validate_did(requester_did: str) -> str:
    """Validate the requester DID against a list of valid DIDs.

    Args:
        requester_did: The DID of the requester to validate.

    Returns:
        The valid requester DID.

    Raises:
        HTTPException: If the DID is not valid.
    """
    print(f"Valid DIDs: {valid_dids}")
    print(f"Requester DID: {requester_did}")
    print(f"Requester DID in valid DIDs: {requester_did in valid_dids}")
    if requester_did not in valid_dids:
        raise HTTPException(status_code=403, detail="Invalid DID")
    return requester_did
