"""Generates training data to be annotated."""


def load_data_to_annotate() -> list[dict]:
    pass


def prelabel_posts(posts: list[dict]) -> list[dict]:
    # batch inference
    # asve outputs so that I don't have to re-do it in the case of errors
    pass


def save_prelabeled_posts(prelabeled_posts: list[dict]) -> None:
    pass


def main() -> None:
    posts: list[dict] = load_data_to_annotate()
    prelabeled_posts: list[dict] = prelabel_posts(posts=posts)
    save_prelabeled_posts(prelabeled_posts=prelabeled_posts)


if __name__ == "__main__":
    main()
