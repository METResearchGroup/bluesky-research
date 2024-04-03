"""Unit tests for bluesky_helper.py

Tests with a real post and author (in this case, the NYTimes). Picked a post
that is unlikely to be deleted or changed, but the tests would fail if that
weren't the case.
"""
from atproto_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed
from atproto_client.models.app.bsky.feed.defs import ThreadViewPost
from atproto_client.models.app.bsky.feed.post import GetRecordResponse
from atproto_client.models.app.bsky.feed.get_likes import Like

# Assuming the functions to test are in your_module.py
from transform.bluesky_helper import (
    get_author_handle_and_post_id_from_link,
    get_author_did_from_handle,
    get_post_record_from_post_link,
    get_repost_profiles,
    get_liked_by_profiles,
    get_post_thread_replies,
    calculate_post_engagement,
    calculate_post_engagement_from_link,
    get_author_profile_from_link
)

link = "https://bsky.app/profile/nytimes.com/post/3kowbajil7r2y"
expected_author_handle = "nytimes.com"
expected_post_id = "3kowbajil7r2y"
expected_post_uri = "at://did:plc:eclio37ymobqex2ncko63h4r/app.bsky.feed.post/3kowbajil7r2y"
expected_author_did = "did:plc:eclio37ymobqex2ncko63h4r"

def test_get_author_handle_and_post_id_from_link():
    res: dict = get_author_handle_and_post_id_from_link(link)
    assert res["author"] == expected_author_handle
    assert res["post_id"] == expected_post_id


def test_get_author_did_from_handle() -> str:
    author_did = get_author_did_from_handle(expected_author_handle)
    assert author_did == expected_author_did


def test_get_post_record_from_post_link():
    record = get_post_record_from_post_link(link)
    assert isinstance(record, GetRecordResponse)

def test_get_repost_profiles():
    reposts = get_repost_profiles(post_uri=expected_post_uri)
    assert isinstance(reposts, list)
    assert len(reposts) > 0
    assert isinstance(reposts[0], ProfileView)

def test_get_liked_by_profiles():
    likes = get_liked_by_profiles(post_uri=expected_post_uri)
    assert isinstance(likes, list)
    assert len(likes) > 0
    assert isinstance(likes[0], Like)

def test_get_post_thread_replies():
    replies = get_post_thread_replies(post_uri=expected_post_uri)
    assert isinstance(replies, list)
    assert len(replies) > 0
    assert isinstance(replies[0], ThreadViewPost)

def test_calculate_post_engagement():
    post_response = get_post_record_from_post_link(link)
    engagement = calculate_post_engagement(post_response)
    assert isinstance(engagement, dict)
    assert "uri" in engagement
    assert "num_likes" in engagement
    assert "num_reposts" in engagement
    assert "num_replies" in engagement

def test_calculate_post_engagement_from_link():
    engagement = calculate_post_engagement_from_link(link)
    assert isinstance(engagement, dict)
    assert "link" in engagement
    assert "created_at" in engagement
    assert "num_likes" in engagement
    assert "num_reposts" in engagement
    assert "num_replies" in engagement

def test_get_author_profile_from_link():
    profile = get_author_profile_from_link(link)
    assert isinstance(profile, ProfileViewDetailed)
    assert profile.handle == expected_author_handle
    assert profile.did == expected_author_did
