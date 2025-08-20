"""
Generate test data for example job processing.

This script creates a JSONL file containing random text entries using the Faker library.
Each entry has a random length between 5 and 50 words. The script is configurable to
generate any number of entries, with a default of 25,000.

The output is saved as 'example.jsonl' in the current directory, with each line
containing a JSON object with 'id', 'group_id', and 'text' fields.
"""

import json
import random
from pathlib import Path
from typing import List, Dict

from faker import Faker


def generate_random_texts(n: int) -> List[Dict[str, str]]:
    """
    Generate n random text entries with lengths between 5 and 50 words.
    Each entry has a random group_id between 1 and 10.

    Args:
        n: Number of text entries to generate

    Returns:
        List of dictionaries containing random text entries
    """
    fake = Faker()
    texts = []

    for i in range(n):
        # Generate random number of words between 5 and 50
        num_words = random.randint(5, 50)
        # Generate random text with specified number of words
        text = fake.text(max_nb_chars=num_words * 5)  # Approximate chars per word
        # Truncate to ensure we don't exceed the word count
        words = text.split()[:num_words]
        text = " ".join(words)

        # Generate random group_id between 1 and 10
        group_id = random.randint(1, 10)

        texts.append({"id": i, "group_id": group_id, "text": text})

    return texts


def save_to_jsonl(data: List[Dict[str, str]], output_path: str) -> None:
    """
    Save data to a JSONL file.

    Args:
        data: List of dictionaries to save
        output_path: Path to the output file
    """
    # Create directory if it doesn't exist
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

    print(f"Successfully wrote {len(data)} entries to {output_path}")


def main(n: int = 25000) -> None:
    """
    Generate n random text entries and save them to a JSONL file.

    Args:
        n: Number of text entries to generate (default: 25,000)
    """
    print(f"Generating {n} random text entries...")
    texts = generate_random_texts(n)

    output_path = "example.jsonl"
    save_to_jsonl(texts, output_path)


if __name__ == "__main__":
    main()
