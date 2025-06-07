from typing import Dict
import json
from ml_tooling.llm.inference import run_query
from pydantic import BaseModel, Field

LLM_INTENT_PROMPT = """
    You are an expert intent classifier for a semantic search engine. 
    Given a user query, classify its intent as one of the following:
      - 'top-k': The user is asking for the most, top, or best items (e.g., most liked posts, top users).
      - 'summarize': The user is asking for a summary or to summarize information.
      - 'unknown': The intent does not match the above categories.
    
    Return a JSON object with two fields:
      - 'intent': one of 'top-k', 'summarize', or 'unknown'
      - 'reason': a short explanation for your classification
    
    User query: ```{query}```
    
    Only return the JSON object, no explanation.
    """


class IntentClassifierResponseModel(BaseModel):
    intent: str = Field(
        ..., description="The classified intent: 'top-k', 'summarize', or 'unknown'."
    )
    reason: str = Field(..., description="A short explanation for the classification.")


def classify_query_intent(query: str) -> Dict[str, str]:
    """
    Use an LLM to classify the intent of a query as 'top-k', 'summarize', or 'unknown'.
    Returns a dict with 'intent' and 'reason'.
    """
    prompt = LLM_INTENT_PROMPT.format(query=query)
    custom_kwargs = {
        "temperature": 0.0,
        "response_format": IntentClassifierResponseModel,
    }
    try:
        response = run_query(
            prompt=prompt,
            role="user",
            model_name="GPT-4o mini",
            num_retries=2,
            custom_kwargs=custom_kwargs,
        )
        # Try to parse the response as JSON
        result = json.loads(response)
        if not ("intent" in result and "reason" in result):
            raise ValueError("Missing required fields in LLM response.")
        return {"intent": result["intent"], "reason": result["reason"]}
    except Exception as e:
        return {"intent": "unknown", "reason": f"LLM classification failed: {e}"}
