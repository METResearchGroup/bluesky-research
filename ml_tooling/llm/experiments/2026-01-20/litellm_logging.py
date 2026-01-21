"""Demonstration script showing LiteLLM logging with default vs suppressed settings.

This script demonstrates the difference between:
1. Using LLMService with verbose=True (does not suppress; uses LiteLLM defaults)
2. Using LLMService with verbose=False (suppresses LiteLLM info/debug logs)

Run this script to see the before/after comparison.
"""

import os
import warnings

from lib.load_env_vars import EnvVarsContainer
from pydantic import BaseModel, Field

# Suppress Pydantic serialization warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

# Ensure Python logging is actually visible. This does NOT change LiteLLM settings;
# it only ensures INFO-level logs have a handler to print to stderr/stdout.
import logging

root_logger = logging.getLogger()
if not root_logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(name)s - %(message)s")

# Suppress retry logger warnings from ml_tooling.llm.retry
retry_logger = logging.getLogger("ml_tooling.llm.retry")
retry_logger.setLevel(logging.ERROR)

# Load environment variables (including OPENAI_API_KEY) from .env file
# This must be done before importing LLMService
api_key = EnvVarsContainer.get_env_var("OPENAI_API_KEY")
if api_key:
    os.environ["OPENAI_API_KEY"] = api_key
    print(f"✓ Loaded OPENAI_API_KEY from .env file (key length: {len(api_key)})\n")
else:
    print("⚠ OPENAI_API_KEY not found in .env file - API calls will fail\n")

# Define a simple response model for structured output
class SimpleResponse(BaseModel):
    """Simple response model for testing."""
    answer: str = Field(description="The answer to the question")
    confidence: float = Field(description="Confidence score between 0 and 1")


# ============================================================================
# Section 1: LLMService with verbose=True (shows verbose logs)
# ============================================================================
print("=" * 80)
print("SECTION 1: LLMService with verbose=True")
print("=" * 80)
print("This section uses LiteLLM DEFAULT logging behavior (not suppressed).\n")
if os.environ.get("LITELLM_LOG"):
    print(
        f"NOTE: Your environment has LITELLM_LOG={os.environ['LITELLM_LOG']!r}. "
        "This can change what LiteLLM prints.\n"
    )

from ml_tooling.llm.llm_service import LLMService  # noqa: E402

# Create service instance with verbose=True (do not suppress; use LiteLLM defaults)
service_verbose = LLMService(verbose=True)

# Make a request using LLMService with verbose logging enabled
print("Making request through LLMService(verbose=True)...")
print("(If LiteLLM emits INFO logs by default, you should see them above this line)\n")

try:
    response = service_verbose.structured_completion(
        messages=[{"role": "user", "content": "Say 'Hello' in one word."}],
        response_model=SimpleResponse,
        model="gpt-4o-mini",
        max_tokens=100,
    )
    print(f"Response (verbose=True): {response.answer}")
    print(f"Confidence: {response.confidence}\n")
except Exception as e:
    print(f"Error: {e}\n")

print("\n" + "=" * 80)
print("END OF SECTION 1")
print("=" * 80 + "\n\n")

# ============================================================================
# Section 2: LLMService with verbose=False (logs suppressed)
# ============================================================================
print("=" * 80)
print("SECTION 2: LLMService with verbose=False (default)")
print("=" * 80)
print("You should see NO verbose logs from LiteLLM below:\n")

# Create service instance with verbose=False (default) - logging IS suppressed
service_suppressed = LLMService(verbose=False)

# Make the same request using LLMService with logging suppressed
print("Making request through LLMService(verbose=False)...")
print("(You should NOT see INFO/DEBUG logs from LiteLLM above this line)\n")

try:
    response = service_suppressed.structured_completion(
        messages=[{"role": "user", "content": "Say 'Hello' in one word."}],
        response_model=SimpleResponse,
        model="gpt-4o-mini",
        max_tokens=100,
    )
    print(f"Response (verbose=False): {response.answer}")
    print(f"Confidence: {response.confidence}\n")
except Exception as e:
    print(f"Error: {e}\n")

print("\n" + "=" * 80)
print("END OF SECTION 2")
print("=" * 80)
print("\nComparison complete!")
print("If you saw verbose logs in Section 1 but not in Section 2, the fix is working!")
