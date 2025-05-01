"""Load users whose posts were either liked or reposted or replied to by
someone in the study.
"""


def get_details_of_posts_liked_by_study_users() -> tuple[set[str], set[str]]:
    """Get the details of posts that were liked by study users.

    Loads likes and then saves the URI of the liked posts as well as the
    DID of the users who wrote those posts that were liked.
    """
    pass


def get_details_of_posts_reposted_by_study_users() -> tuple[set[str], set[str]]:
    """Get the details of posts that were reposted by study users.

    Loads reposts and then saves the URI of the reposted posts as well as the
    DID of the users who wrote those posts that were reposted.
    """
    pass


def get_details_of_posts_replied_to_by_study_users() -> tuple[set[str], set[str]]:
    """Get the details of posts that were replied to by study users.

    Loads replies and then saves the (1) URIs of the posts in the parent
    and root posts in the thread, and (2) the DIDs of the users who wrote
    those posts.
    """
    pass


def main():
    dids_of_liked_posts, uris_of_liked_posts = (
        get_details_of_posts_liked_by_study_users()
    )
    dids_of_reposted_posts, uris_of_reposted_posts = (
        get_details_of_posts_reposted_by_study_users()
    )
    dids_of_replied_posts, uris_of_replied_posts = (
        get_details_of_posts_replied_to_by_study_users()
    )

    dids_of_users_engaged_with = (
        dids_of_liked_posts + dids_of_reposted_posts + dids_of_replied_posts
    )
    uris_of_posts_engaged_with = (
        uris_of_liked_posts + uris_of_reposted_posts + uris_of_replied_posts
    )

    print(f"Total number of users engaged with: {len(dids_of_users_engaged_with)}")
    print(f"Total number of posts engaged with: {len(uris_of_posts_engaged_with)}")

    # TODO: export these to SQLite.
    pass


if __name__ == "__main__":
    main()
