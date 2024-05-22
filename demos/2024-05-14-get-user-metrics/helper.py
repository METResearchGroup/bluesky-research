from atproto_client.models.app.bsky.actor.defs import (
    ProfileView, ProfileViewDetailed
)

from lib.helper import client
from transform.bluesky_helper import get_user_followers

example_link = "https://bsky.app/profile/cola.baby"


def get_handle_from_link(link: str) -> str:
    return link.split("/")[-1]


def get_profile_metrics(profile: ProfileViewDetailed) -> dict:
    return {
        "handle": profile.handle,
        "followers_count": profile.followers_count,
        "follows_count": profile.follows_count,
        "posts_count": profile.posts_count,
    }


def main():
    handle = example_link.split("/")[-1]
    profile: ProfileViewDetailed = client.get_profile(handle)
    followers: list[ProfileView] = get_user_followers(handle=handle)
    print(f"Handle: {handle}\tProfile: {profile}\tFollowers: {followers}")


if __name__ == "__main__":
    main()
