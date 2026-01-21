"""Demonstration script showing LiteLLM logging before and after suppression.

This script demonstrates the difference between:
1. Using LiteLLM directly (shows verbose info/debug logs)
2. Using LiteLLM through our module (logs are suppressed)

Run this script to see the before/after comparison.
"""

from pydantic import BaseModel, Field

# ============================================================================
# Section 1: BEFORE FIX - Direct LiteLLM import (shows verbose logs)
# ============================================================================
print("=" * 80)
print("SECTION 1: BEFORE FIX - Direct LiteLLM import")
print("=" * 80)
print("You should see verbose INFO/DEBUG logs from LiteLLM below:\n")

# Import litellm directly (bypassing our module's logging configuration)
import litellm  # noqa: E402

# Define a simple response model for structured output
class SimpleResponse(BaseModel):
    """Simple response model for testing."""
    answer: str = Field(description="The answer to the question")
    confidence: float = Field(description="Confidence score between 0 and 1")


# Make a minimal request using direct litellm.completion
# This will show verbose logs because we haven't configured logging suppression
print("Making request with direct litellm import...")
print("(Look for INFO/DEBUG logs from LiteLLM above this line)\n")

try:
    response = litellm.completion(
        model="gpt-5-nano",
        messages=[{"role": "user", "content": "Say 'Hello' in one word."}],
        max_tokens=10,
    )
    print(f"Response (direct import): {response.choices[0].message.content}\n")
except Exception as e:
    print(f"Error (this is expected if API keys aren't configured): {e}\n")

print("\n" + "=" * 80)
print("END OF SECTION 1")
print("=" * 80 + "\n\n")

# ============================================================================
# Section 2: AFTER FIX - Using LLMService (logs suppressed)
# ============================================================================
print("=" * 80)
print("SECTION 2: AFTER FIX - Using LLMService")
print("=" * 80)
print("You should see NO verbose logs from LiteLLM below:\n")

# Now import and use LLMService, which triggers logging suppression in __init__
from ml_tooling.llm.llm_service import LLMService  # noqa: E402

# Create service instance (this triggers logging suppression in __init__)
service = LLMService()

# Make the same request using LLMService
# This should NOT show verbose logs because logging is suppressed in LLMService.__init__
print("Making request through LLMService (logging should be suppressed)...")
print("(You should NOT see INFO/DEBUG logs from LiteLLM above this line)\n")

try:
    response = service.structured_completion(
        messages=[{"role": "user", "content": "Say 'Hello' in one word."}],
        response_model=SimpleResponse,
        model="gpt-4o-mini",
        max_tokens=10,
    )
    print(f"Response (through module): {response.answer}")
    print(f"Confidence: {response.confidence}\n")
except Exception as e:
    print(f"Error (this is expected if API keys aren't configured): {e}\n")

print("\n" + "=" * 80)
print("END OF SECTION 2")
print("=" * 80)
print("\nComparison complete!")
print("If you saw verbose logs in Section 1 but not in Section 2, the fix is working!")
