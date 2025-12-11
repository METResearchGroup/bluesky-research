def extract_uri_suffix(uri: str) -> str:
    """Extract post URI suffix from full URI.

    Example:
        Input: "at://did:plc:abc123/app.bsky.feed.post/3kwd3wuubke2i"
        Output: "3kwd3wuubke2i"

    An example URI is something like "at://did:plc:abc123/app.bsky.feed.post/3kwd3wuubke2i".
    The common format is "at://<did>/<app.bsky.feed.post>/<post_id>", and
    the helper tool just needs to grab "<post_id>".

    Args:
        uri: Full URI

    Returns:
        URI suffix (last component after final '/')
    """
    return uri.split("/")[-1]


def build_record_filename(record_type: str, author_did: str, uri_suffix: str) -> str:
    """Build filename for record.

    Args:
        record_type: Record type
        author_did: Author DID
        uri_suffix: URI suffix

    Returns:
        Filename string
    """
    return f"{record_type}_author_did={author_did}_uri_suffix={uri_suffix}.json"
