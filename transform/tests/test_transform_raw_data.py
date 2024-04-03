import pytest
import json

from atproto_client.models.app.bsky.embed.external import External, Main as ExternalEmbed # noqa
from atproto_client.models.app.bsky.embed.images import Image, Main as ImageEmbed # noqa
from atproto_client.models.app.bsky.embed.record import Main as RecordEmbed
from atproto_client.models.app.bsky.embed.record_with_media import Main as RecordWithMediaEmbed # noqa

# Assuming the functions to test are in helper.py
from transform.transform_raw_data import (
    process_mention, process_link, process_hashtag, process_facet,
    process_facets, process_label, process_labels, process_entity,
    process_entities, process_replies, process_image, process_images,
    process_strong_ref, process_record_embed, process_external_embed,
    process_record_with_media_embed, process_embed
)

# Mock classes
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
    def __init__(self, description, title, url):
        self.description = description
        self.title = title
        self.url = url
        self.py_type = 'app.bsky.embed.external#external'

class RecordWithMediaEmbed:
    def __init__(self, media, record):
        self.media = media
        self.record = record
        self.py_type = "app.bsky.embed.recordWithMedia"

# Tests
def test_process_mention():
    mention = Mention(did="did:plc:beitbj4for3pe4babgntgfjc")
    assert process_mention(mention) == "mention:did:plc:beitbj4for3pe4babgntgfjc"

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
    assert process_facet(facet) == "mention:did:plc:beitbj4for3pe4babgntgfjc,link:https://example.com,tag:red"

def test_process_facets():
    mention = Mention(did="did:plc:beitbj4for3pe4babgntgfjc")
    link = Link(uri="https://example.com")
    tag = Tag(tag="red")
    facet1 = Facet(features=[mention, link])
    facet2 = Facet(features=[tag])
    assert process_facets([facet1, facet2]) == "mention:did:plc:beitbj4for3pe4babgntgfjc,link:https://example.com,tag:red"

def test_process_label():
    label = SelfLabel(val="porn", py_type="com.atproto.label.defs#selfLabel")
    assert process_label(label) == "porn"

def test_process_labels():
    label = SelfLabel(val="porn", py_type="com.atproto.label.defs#selfLabel")
    labels = SelfLabels(values=[label])
    assert process_labels(labels) == "porn"

def test_process_entity():
    entity = Entity(index=None, type="link", value="https://example.com", py_type="app.bsky.feed.post#entity")
    assert process_entity(entity) == "https://example.com"

def test_process_entities():
    entity = Entity(index=None, type="link", value="https://example.com", py_type="app.bsky.feed.post#entity")
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
    assert process_strong_ref(strong_ref) == {"cid": "1234", "uri": "http://example.com"}

def test_process_record_embed():
    strong_ref = StrongRef(cid="1234", uri="http://example.com")
    record_embed = RecordEmbed(record=strong_ref)
    assert json.loads(process_record_embed(record_embed)) == {"cid": "1234", "uri": "http://example.com"}

def test_process_external_embed():
    external = External(description="Description", title="Title", url="http://example.com")
    external_embed = ExternalEmbed(external=external)
    assert json.loads(process_external_embed(external_embed)) == {"description": "Description", "title": "Title", "url": "http://example.com"}

def test_process_record_with_media_embed():
    image = ImageEmbed([Image(alt="An image")])
    strong_ref = StrongRef(cid="1234", uri="http://example.com")
    record_embed = RecordEmbed(record=strong_ref)
    record_with_media = RecordWithMediaEmbed(media=image, record=record_embed)
    result = process_record_with_media_embed(record_with_media)
    assert result == {"image_alt_text": "An image", "embedded_record": json.dumps({"cid": "1234", "uri": "http://example.com"})}

def test_process_embed():
    image = ImageEmbed([Image(alt="An image")])
    processed_embed = process_embed(image)
    expected_embed = {"has_image": True, "image_alt_text": "An image", "has_embedded_record": False, "embedded_record": None, "has_external": False, "external": None}
    assert json.loads(processed_embed) == expected_embed
