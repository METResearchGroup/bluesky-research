"""Vectorizes the articles in the DB.

Token estimator: https://platform.openai.com/tokenizer
    Looks like text + captions + abstract ~ 75-100 tokens, 300-400 chars?

RAG references:
    - https://huggingface.co/learn/cookbook/en/advanced_rag
    - https://python.langchain.com/docs/modules/data_connection/vectorstores/
"""
from typing import Optional

from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores.faiss import FAISS
from langchain_community.vectorstores.utils import DistanceStrategy
from langchain.docstore.document import Document as LangchainDocument
import pandas as pd

from services.classify_civic.update_current_events.sync_nytimes_articles import load_all_articles_as_df # noqa

MAX_CHUNK_SIZE = 512
DEFAULT_EMBEDDING_MODEL = "thenlper/gte-small"
VECTOR_DB_NAME = "faiss_index_nytimes"

embedding_model = HuggingFaceEmbeddings(
    model_name=DEFAULT_EMBEDDING_MODEL,
    multi_process=True,
    #model_kwargs={"device": "cuda"},
    encode_kwargs={"normalize_embeddings": True},  # set True for cosine similarity
)


def load_all_articles_as_documents() -> list[LangchainDocument]:
    """Load all existing articles and preprocess them.
    
    Returns a list of LangchainDocument objects.
    """
    all_articles_df: pd.DataFrame = load_all_articles_as_df()
    all_articles_df["full_text"] = (
        all_articles_df["title"] + " "
        + all_articles_df["abstract"] + " "
        + all_articles_df["captions"]
    )
    # to make sure that the text conforms to our tokenizer character limits.
    # alternatively, we could use RecursiveCharacterTextSplitter if our text
    # starts getting too long.
    all_articles_df["full_text_truncated"] = all_articles_df["full_text"].apply(
        lambda x: x[:MAX_CHUNK_SIZE]
    )
    all_articles_dict_list = all_articles_df.to_dict(orient="records")
    raw_knowledge_base: list[LangchainDocument] = [
        LangchainDocument(
            page_content=article["full_text_truncated"],
            metadata={
                "id": article["id"], "nytimes_uri": article["nytimes_uri"]
            }
        )
        for article in all_articles_dict_list
    ]
    return raw_knowledge_base


def create_vector_database(
    raw_knowledge_base: list[LangchainDocument],
    embedding_model: HuggingFaceEmbeddings,
    distance_strategy: DistanceStrategy = DistanceStrategy.COSINE
) -> FAISS:
    """Creates a vector database from the raw knowledge base."""
    return FAISS.from_documents(
        raw_knowledge_base, embedding_model, distance_strategy=distance_strategy
    )


def get_or_create_vector_database(
    database_name: str,
    embedding_model: HuggingFaceEmbeddings,
    raw_knowledge_base: Optional[list[LangchainDocument]] = None
) -> FAISS:
    """Get or create a FAISS vector database."""
    try:
        return FAISS.load_local(database_name, embedding_model)
    except FileNotFoundError:
        create_vector_database(raw_knowledge_base, embedding_model)


if __name__ == "__main__":
    raw_knowledge_base: list[LangchainDocument] = (
        load_all_articles_as_documents()
    )
    vector_db = get_or_create_vector_database(
        database_name=VECTOR_DB_NAME,
        embedding_model=embedding_model,
        raw_knowledge_base=raw_knowledge_base
    )
    print("Finished creating vector database.")
