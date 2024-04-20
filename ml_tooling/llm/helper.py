"""Helper LLM tooling."""
import tiktoken

enc = tiktoken.get_encoding("cl100k_base")


def tokenize_text(text: str):
    """Uses tiktoken to tokenize text.

    Source example from OpenAI:
    - https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
    """ # noqa
    pass


def decode_tokenized_text(tokenized_text: list[int]) -> str:
    """Decodes tokenized text."""
    pass


def generate_enumerated_input_list(
    texts: list[str], max_token_length: int
) -> tuple:
    """Generates an enumerated list of inputs.

    Returns a tuple of results and the number of enumerated values.

    Example:
    >> generate_enumerated_input_list(["a", "b", "c"], 100)
    """


if __name__ == "__main__":
    pass
