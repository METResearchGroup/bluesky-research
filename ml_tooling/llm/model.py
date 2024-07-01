"""Model for classifying posts using LLMs."""
from langchain.output_parsers import RetryOutputParser
from langchain_core.output_parsers import JsonOutputParser
from langchain_community.chat_models import ChatLiteLLM

from ml_tooling.llm.inference import BACKEND_OPTIONS
from services.ml_inference.models import LLMSociopoliticalLabelModel

DEFAULT_BATCH_SIZE = 10
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TASK_NAME = "civic_and_political_ideology"
LLM_MODEL_NAME = BACKEND_OPTIONS["Gemini"]["model"]  # "gemini/gemini-pro"

chat_model = ChatLiteLLM(model="gemini/gemini-1.0-pro-latest")
parser = JsonOutputParser(pydantic_object=LLMSociopoliticalLabelModel)

retry_parser = RetryOutputParser.from_llm(parser=parser, llm=chat_model)
