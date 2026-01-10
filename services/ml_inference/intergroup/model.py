class IntergroupClassifier:
    def __init__(self):
        from ml_tooling.llm.llm_service import LLMService, get_llm_service

        self.llm_service: LLMService = get_llm_service()

    # TODO: add stronger typing for input and output.
    def classify_posts(self, posts: list[dict]) -> list[dict]:
        """Public-facing API for classifying posts."""
        return []
