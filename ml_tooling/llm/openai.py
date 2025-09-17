#!/usr/bin/env python3
"""
OpenAI API integration for topic modeling.

This module provides a simple interface to OpenAI's GPT models for generating
meaningful topic names from topic keywords.
"""

from openai import OpenAI

from lib.helper import OPENAI_API_KEY


def run_query(
    prompt: str,
    model: str = "gpt-4o-mini",
    max_tokens: int = 150,
    temperature: float = 0.7,
) -> str:
    """
    Run a query against OpenAI's API.

    Args:
        prompt: The prompt to send to the model
        model: The model to use (default: gpt-4o-mini)
        max_tokens: Maximum tokens in response (default: 150)
        temperature: Sampling temperature (default: 0.7)

    Returns:
        The model's response as a string

    Raises:
        ValueError: If OPENAI_API_KEY is not set
        Exception: If the API call fails
    """
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is not set")

    client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        completion = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
        )

        return completion.choices[0].message.content.strip()

    except Exception as e:
        raise Exception(f"OpenAI API call failed: {str(e)}")


if __name__ == "__main__":
    # Test the run_query function
    test_prompt = "Generate a brief category name for social media posts about: football, soccer, game, team, player"

    try:
        result = run_query(test_prompt, max_tokens=50, temperature=0.3)
        print(f"Generated category: {result}")
    except Exception as e:
        print(f"Test failed: {e}")
