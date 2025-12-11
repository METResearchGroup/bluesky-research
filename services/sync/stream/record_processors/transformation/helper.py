from services.sync.stream.types import Operation, RecordType


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


def build_record_filename(
    record_type: RecordType, operation: Operation, author_did: str, uri_suffix: str
) -> str:
    """Build filename for record.

    Args:
        record_type: Record type
        operation: Operation
        author_did: Author DID
        uri_suffix: URI suffix

    Returns:
        Filename string.

    For CREATE operations, the filename is something like:
        author_did=did:plc:abc123_post_uri_suffix=3kwd3wuubke2i.json (the record type and author DID are included)
    For DELETE operations, the filename is something like:
        post_uri_suffix=3kwd3wuubke2i.json (only the record type and URI suffix are needed)
    """
    if operation == Operation.CREATE:
        return (
            f"author_did={author_did}_{record_type.value}_uri_suffix={uri_suffix}.json"
        )
    elif operation == Operation.DELETE:
        return f"{record_type.value}_uri_suffix={uri_suffix}.json"
    else:
        raise ValueError(f"Unknown operation: {operation}")
