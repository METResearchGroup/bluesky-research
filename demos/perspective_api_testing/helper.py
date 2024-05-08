from demos.perspective_api_testing.models import PerspectiveAPIClassification


def load_posts() -> list[dict]:
    # TODO: write in the MongoDB helpers.
    # TODO: write into a model.
    pass


def preprocess_posts() -> list[dict]:
    pass


def classify_post(post: dict) -> PerspectiveAPIClassification:
    pass


def classify_posts(posts: list[dict]) -> list[PerspectiveAPIClassification]:
    pass


def load_and_classify_posts() -> list[dict]:
    posts: list[dict] = load_posts()
