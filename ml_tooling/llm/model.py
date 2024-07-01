"""Model for classifying posts using LLMs."""
from langchain_community.chat_models import ChatLiteLLM
from langchain_community.llms import LlamaCpp
from langchain_core.callbacks import CallbackManager, StreamingStdOutCallbackHandler

from ml_tooling.llm.inference import BACKEND_OPTIONS

DEFAULT_BATCH_SIZE = 10
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TASK_NAME = "civic_and_political_ideology"
LLM_MODEL_NAME = BACKEND_OPTIONS["Gemini"]["model"]  # "gemini/gemini-pro"

callback_manager = CallbackManager([StreamingStdOutCallbackHandler()])

# model
llama_model_path = "/kellogg/software/llama_cpp/models/llama-2-7b-chat.Q5_K_M.gguf"  # noqa

# settings
context_size = 512
threads = 1
gpu_layers = -1
max_tokens_select = 1000
temperature_select: float = 0
top_p_select: float = 0.9
top_k_select: int = 0
include_prompt = False


def get_llm_model(local: bool = False):
    """Define the model used for inference.

    If local, we will use KLC's hosted Llama model. Else we'll use an external
    API, such as Gemini.

    Sources:
    - https://python.langchain.com/v0.2/docs/integrations/llms/llamacpp/
    - https://github.com/rs-kellogg/krs-openllm-cookbook/blob/main/scripts/llama_cpp_python/llama2_ex.py
    """  # noqa
    if local:
        return LlamaCpp(
            model_path=llama_model_path,
            n_ctx=context_size,
            n_threads=threads,
            n_gpu_layers=gpu_layers,
            temperature=1,
            max_tokens=2000,
            top_p=1,
            callback_manager=callback_manager,
            verbose=True,  # Verbose required to pass to the callback manager
        )
    return ChatLiteLLM(model="gemini/gemini-1.0-pro-latest")
