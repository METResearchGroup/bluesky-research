"""Create Bluesky classes from dict."""
from atproto_client.models.app.bsky.richtext.facet import (
    Byteslice, Main as Facet, Link, Mention, Tag
)


def create_link(link_dict: dict) -> Link:
    return Link(uri=link_dict["uri"])


def create_mention(mention_dict: dict) -> Mention:
    return Mention(did=mention_dict["did"])


def create_tag(tag_dict: dict) -> Tag:
    return Tag(tag=tag_dict["tag"])


def create_byteslice(index: dict) -> Byteslice:
    return Byteslice(
        byte_start=index["byte_start"], byte_end=index["byte_end"]
    )


def process_features(features: list) -> list:
    processed_features: list = []
    for feature in features:
        if feature["py_type"] == "app.bsky.richtext.facet#link":
            processed_features.append(create_link(feature))
        elif feature["py_type"] == "app.bsky.richtext.facet#mention":
            processed_features.append(create_mention(feature))
        elif feature["py_type"] == "app.bsky.richtext.facet#tag":
            processed_features.append(create_tag(feature))
    return processed_features


def create_facet(facet_dict: dict) -> Facet:
    features = process_features(facet_dict["features"])
    index = create_byteslice(facet_dict["index"])
    return Facet(features=features, index=index)


def create_facets(facets: list[dict]) -> list[Facet]:
    return [create_facet(facet) for facet in facets]
