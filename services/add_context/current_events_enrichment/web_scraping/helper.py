"""Scrapes web pages using ScrapeGraph

GitHub repo: https://github.com/VinciGit00/Scrapegraph-ai
"""
from lib.helper import GROQ_API_KEY
from ml_tooling.llm.inference import BACKEND_OPTIONS

model_name = "Llama3-70b (via Groq)"

graph_config = {
    "llm": {
        "model": BACKEND_OPTIONS[model_name]["model"],
        "api_key": GROQ_API_KEY,
        "temperature": 0
    },
    "embeddings": {
        "model": "hugging_face/roberta-base",
    },
    "verbose": True,
    "max_results": 5
}
