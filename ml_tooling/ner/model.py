def get_entities_for_post(post: str) -> list[dict[str, str]]:
    """Retrieves the entities for a given post.

    Returns the entity as well as entity type.
    """
    return []


def get_entities_for_posts(
    uri_to_text: dict[str, str],
) -> dict[str, list[dict[str, str]]]:
    uri_to_entities_map: dict[str, list[dict[str, str]]] = {}
    for uri, text in uri_to_text.items():
        entities: list[dict[str, str]] = get_entities_for_post(text)
        uri_to_entities_map[uri] = entities
    return uri_to_entities_map
