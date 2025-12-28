"""Tests for `transform_raw_data.py`."""
import json
import sys
from unittest.mock import MagicMock

# Mock boto3 before importing modules that depend on it
sys.modules["boto3"] = MagicMock()

import pytest
from atproto_client.models.app.bsky.feed.post import Record

from transform.bluesky_helper import get_post_record_from_post_link
from transform.transform_raw_data import (
    process_mention, process_link, process_hashtag, process_facet,
    process_facets, process_label, process_labels, process_entity,
    process_entities, process_replies, process_image, process_images,
    process_strong_ref, process_record_embed, process_external_embed,
    process_record_with_media_embed, process_embed, process_firehose_post
)


class Mention:
    def __init__(self, did):
        self.did = did
        self.py_type = 'app.bsky.richtext.facet#mention'


class Link:
    def __init__(self, uri):
        self.uri = uri
        self.py_type = 'app.bsky.richtext.facet#link'


class Tag:
    def __init__(self, tag):
        self.tag = tag
        self.py_type = 'app.bsky.richtext.facet#tag'


class Facet:
    def __init__(self, features):
        self.features = features


class Feature:
    def __init__(self, py_type, tag=None, uri=None, did=None):
        self.py_type = py_type
        self.tag = tag
        self.uri = uri
        self.did = did


class SelfLabel:
    def __init__(self, val, py_type):
        self.val = val
        self.py_type = py_type


class SelfLabels:
    def __init__(self, values):
        self.values = values


class Entity:
    def __init__(self, index, type, value, py_type):
        self.index = index
        self.type = type
        self.value = value
        self.py_type = py_type


class ReplyRef:
    def __init__(self, parent=None, root=None):
        self.parent = parent
        self.root = root


class ReplyParent:
    def __init__(self, uri):
        self.uri = uri


class ReplyRoot:
    def __init__(self, uri):
        self.uri = uri


class Image:
    def __init__(self, alt):
        self.alt = alt
        self.py_type = "app.bsky.embed.images#image"


class ImageEmbed:
    def __init__(self, images):
        self.images = images
        self.py_type = "app.bsky.embed.images"


class StrongRef:
    def __init__(self, cid, uri):
        self.cid = cid
        self.uri = uri


class RecordEmbed:
    def __init__(self, record):
        self.record = record
        self.py_type = "app.bsky.embed.record"


class ExternalEmbed:
    def __init__(self, external):
        self.external = external
        self.py_type = 'app.bsky.embed.external'


class External:
    def __init__(self, description, title, uri):
        self.description = description
        self.title = title
        self.uri = uri
        self.py_type = 'app.bsky.embed.external#external'


class RecordWithMediaEmbed:
    def __init__(self, media, record):
        self.media = media
        self.record = record
        self.py_type = "app.bsky.embed.recordWithMedia"


def test_process_mention():
    mention = Mention(did="did:plc:beitbj4for3pe4babgntgfjc")
    assert process_mention(
        mention) == "mention:did:plc:beitbj4for3pe4babgntgfjc"


def test_process_link():
    link = Link(uri="https://example.com")
    assert process_link(link) == "link:https://example.com"


def test_process_hashtag():
    tag = Tag(tag="red")
    assert process_hashtag(tag) == "tag:red"


def test_process_facet():
    mention = Mention(did="did:plc:beitbj4for3pe4babgntgfjc")
    link = Link(uri="https://example.com")
    tag = Tag(tag="red")
    facet = Facet(features=[mention, link, tag])
    assert (
        process_facet(facet)
        == "mention:did:plc:beitbj4for3pe4babgntgfjc;link:https://example.com;tag:red"  # noqa
    )


def test_process_facets():
    mention = Mention(did="did:plc:beitbj4for3pe4babgntgfjc")
    link = Link(uri="https://example.com")
    tag = Tag(tag="red")
    facet1 = Facet(features=[mention, link])
    facet2 = Facet(features=[tag])
    assert (
        process_facets([facet1, facet2])
        == "mention:did:plc:beitbj4for3pe4babgntgfjc;link:https://example.com;tag:red"  # noqa
    )


def test_process_label():
    label = SelfLabel(val="porn", py_type="com.atproto.label.defs#selfLabel")
    assert process_label(label) == "porn"


def test_process_labels():
    label = SelfLabel(val="porn", py_type="com.atproto.label.defs#selfLabel")
    labels = SelfLabels(values=[label])
    assert process_labels(labels) == "porn"


def test_process_entity():
    entity = Entity(index=None, type="link", value="https://example.com",
                    py_type="app.bsky.feed.post#entity")
    assert process_entity(entity) == "https://example.com"


def test_process_entities():
    entity = Entity(index=None, type="link", value="https://example.com",
                    py_type="app.bsky.feed.post#entity")
    assert process_entities([entity]) == "https://example.com"


def test_process_replies():
    parent = ReplyParent(uri="parent-uri")
    root = ReplyRoot(uri="root-uri")
    reply_ref = ReplyRef(parent=parent, root=root)
    result = process_replies(reply_ref)
    assert result == {"reply_parent": "parent-uri", "reply_root": "root-uri"}


def test_process_image():
    image = Image(alt="An image")
    assert process_image(image) == "An image"


def test_process_images():
    images = ImageEmbed([Image(alt="Image 1"), Image(alt="Image 2")])
    assert process_images(images) == "Image 1;Image 2"


def test_process_strong_ref():
    strong_ref = StrongRef(cid="1234", uri="http://example.com")
    result = process_strong_ref(strong_ref)
    expected_result = {
        "cid": "1234", "uri": "http://example.com"
    }
    assert result == expected_result


def test_process_record_embed():
    """Test that process_record_embed returns ProcessedRecordEmbed model."""
    # Arrange
    strong_ref = StrongRef(cid="1234", uri="http://example.com")
    record_embed = RecordEmbed(record=strong_ref)
    expected = {
        "cid": "1234", "uri": "http://example.com"
    }
    
    # Act
    result = process_record_embed(record_embed)
    
    # Assert
    assert result.model_dump() == expected


def test_process_external_embed():
    """Test that process_external_embed returns ProcessedExternalEmbed model."""
    # Arrange
    external = External(description="Description",
                        title="Title", uri="http://example.com")
    external_embed = ExternalEmbed(external=external)
    expected = {
        "description": "Description",
        "title": "Title",
        "uri": "http://example.com"
    }
    
    # Act
    result = process_external_embed(external_embed)
    
    # Assert
    assert result.model_dump() == expected


def test_process_record_with_media_embed():
    """Test that process_record_with_media_embed returns ProcessedRecordWithMediaEmbed model."""
    # Arrange
    image = ImageEmbed([Image(alt="An image")])
    strong_ref = StrongRef(cid="1234", uri="http://example.com")
    record_embed = RecordEmbed(record=strong_ref)
    record_with_media = RecordWithMediaEmbed(media=image, record=record_embed)
    expected_embedded_record = {
        "cid": "1234", "uri": "http://example.com"
    }
    
    # Act
    result = process_record_with_media_embed(record_with_media)
    
    # Assert
    assert result.image_alt_text == "An image"
    assert result.embedded_record.model_dump() == expected_embedded_record


def test_process_embed():
    """Test that process_embed returns ProcessedEmbed model."""
    # Arrange
    image = ImageEmbed([Image(alt="An image")])
    expected = {
        "has_image": True,
        "image_alt_text": "An image",
        "has_embedded_record": False,
        "embedded_record": None,
        "has_external": False,
        "external": None
    }
    
    # Act
    result = process_embed(image)
    
    # Assert
    assert result.model_dump() == expected


@pytest.mark.integration
def test_process_firehose_post():
    """Test our ability to flatten a firehost post.

    To test this, we'll use real posts. We'll then flatten it and compare the
    result to the expected result.

    Uses posts from the NYTimes, as these will be unlikely to change or be
    deleted, plus they let us test for the existence of things like embeds
    and facets, which are the most tricky to parse since they have different
    variations.
    
    This is an integration test that makes real API calls.
    """  # noqa
    # test 1
    link = "https://bsky.app/profile/nytimes.com/post/3kowbajil7r2y"
    record = get_post_record_from_post_link(link)
    record_value = record.value
    # available at record.uri
    expected_post_uri = "at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3kowbajil7r2y"  # noqa
    # available at record.cid
    expected_post_cid = "bafyreicmozjihgi5fbc76r6nfxdv4bvrhnpbglkfpzy7uv6l4suvxw2miu"  # noqa
    expected_post_author_did = "did:plc:eclio37ymobqex2ncko63h4r"
    post = {
        "record": record_value,
        "uri": expected_post_uri,
        "cid": expected_post_cid,
        "author": expected_post_author_did
    }
    assert isinstance(record_value, Record)
    assert isinstance(post, dict)
    flattened_firehose_post = process_firehose_post(post)
    # we don't need to test for this
    flattened_firehose_post.pop("synctimestamp")
    expected_flattened_firehose_post = {
        'author': 'did:plc:eclio37ymobqex2ncko63h4r',
        'cid': 'bafyreicmozjihgi5fbc76r6nfxdv4bvrhnpbglkfpzy7uv6l4suvxw2miu',  # noqa
        'created_at': '2024-03-30T14:44:59.095Z',
        'embed': '{"has_image": false, "image_alt_text": null, "has_embedded_record": '  # noqa
        'false, "embedded_record": null, "has_external": true, "external": '
        '"{\\"description\\": \\"With Democrats holding a one-seat majority '
        'and defending seats from Maryland to Arizona, control of the Senate '
        'could easily flip to the G.O.P.\\", \\"title\\": \\"10 Senate Races '
        'to Watch in 2024\\", \\"uri\\": '
        '\\"https://www.nytimes.com/article/senate-races-2024-election.html?smtyp=cur&smid=bsky-nytimes\\"}"}',  # noqa
        'entities': None,
        'facets': None,
        'labels': None,
        'langs': 'en',
        'py_type': 'app.bsky.feed.post',
        'reply_parent': None,
        'reply_root': None,
        'tags': None,
        'text': 'With Democrats holding a one-seat majority and defending seats from '  # noqa
                'Maryland to Arizona, control of the Senate could easily flip to the '  # noqa
                'Republicans. Here are the Senate races to watch in 2024.',
        'uri': 'at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3kowbajil7r2y'  # noqa
    }
    assert flattened_firehose_post == expected_flattened_firehose_post

    # test 2:
    link = "https://bsky.app/profile/nytimes.com/post/3koueui3yld24"
    record = get_post_record_from_post_link(link)
    record_value = record.value
    # available at record.uri
    expected_post_uri = "at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3koueui3yld24"  # noqa
    # available at record.cid
    expoected_post_cid = "bafyreieuyk7ga7ldbvn3ayhe3c3ntnz7rlbgdqlfnp53v2wwjaanmrl3dm"  # noqa
    expected_post_author_did = "did:plc:eclio37ymobqex2ncko63h4r"
    post = {
        "record": record_value,
        "uri": expected_post_uri,
        "cid": expoected_post_cid,
        "author": expected_post_author_did
    }
    assert isinstance(record_value, Record)
    assert isinstance(post, dict)
    flattened_firehose_post = process_firehose_post(post)
    # we don't need to test for this
    flattened_firehose_post.pop("synctimestamp")
    expected_flattened_firehose_post = {
        'author': 'did:plc:eclio37ymobqex2ncko63h4r',
        'cid': 'bafyreieuyk7ga7ldbvn3ayhe3c3ntnz7rlbgdqlfnp53v2wwjaanmrl3dm',
        'created_at': '2024-03-29T20:44:30.388Z',
        'embed': '{"has_image": true, "image_alt_text": "UnitedHealth Group\\u2019s '  # noqa
        'headquarters in Minnetonka, Minnesota. A headline reads: \\"What to '
        'Know About Health Care Cyberattacks.\\" Photo by Unitedhealth '
        'Group, via Reuters.", "has_embedded_record": false, '
        '"embedded_record": null, "has_external": false, "external": null}',
        'entities': None,
        'facets': 'link:https://www.nytimes.com/2024/03/29/health/cyber-attack-unitedhealth-hospital-patients.html?smtyp=cur&smid=bsky-nytimes',  # noqa
        'labels': None,
        'langs': 'en',
        'py_type': 'app.bsky.feed.post',
        'reply_parent': None,
        'reply_root': None,
        'tags': None,
        'text': 'A cyberattack shut down the largest U.S. health care payment system '  # noqa
                "last month. Here's what to know as hospitals, health insurers, "  # noqa
                'physician clinics and others in the industry increasingly become the '  # noqa
                'targets of significant hacks. www.nytimes.com/2024/03/29/h...',  # noqa
        'uri': 'at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3koueui3yld24'  # noqa
    }
    assert flattened_firehose_post == expected_flattened_firehose_post
