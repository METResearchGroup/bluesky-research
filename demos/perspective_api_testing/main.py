from demos.perspective_api_testing.helper import load_and_classify_posts


def main() -> None:
    classified_posts: list[dict] = load_and_classify_posts()
    print(f"Classified posts: {classified_posts}")


if __name__ == "__main__":
    pass
