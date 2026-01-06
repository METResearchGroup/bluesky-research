"""Helper LLM tooling."""
import tiktoken

enc = tiktoken.get_encoding("cl100k_base")


def tokenize_text(text: str) -> list[int]:
    """Uses tiktoken to tokenize text.

    Source example from OpenAI:
    - https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
    """  # noqa
    return enc.encode(text)


def decode_tokenized_text(tokenized_text: list[int]) -> str:
    """Decodes tokenized text."""
    return enc.decode(tokenized_text)


def generate_enumerated_input_list(
    texts: list[str], max_token_length: int
) -> tuple[str, int]:
    """Generates an enumerated list of inputs.

    Returns a tuple of results and the number of enumerated values.

    Example:
    >> generate_enumerated_input_list(["a", "b", "c"], 100)
    """
    if max_token_length <= 0:
        raise ValueError("max_token_length must be > 0")

    lines: list[str] = []
    for i, text in enumerate(texts, start=1):
        candidate = "\n".join([*lines, f"{i}. {text}"])
        if len(enc.encode(candidate)) > max_token_length:
            break
        lines.append(f"{i}. {text}")

    return "\n".join(lines), len(lines)


if __name__ == "__main__":
    pass
