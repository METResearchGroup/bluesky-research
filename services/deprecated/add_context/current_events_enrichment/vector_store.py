"""Vectorizes the articles in the DB.

Token estimator: https://platform.openai.com/tokenizer
    Looks like text + captions + abstract ~ 75-100 tokens, 300-400 chars?

RAG and vector store references:
    - https://huggingface.co/learn/cookbook/en/advanced_rag
    - https://python.langchain.com/docs/modules/data_connection/vectorstores/
    - https://python.langchain.com/docs/integrations/vectorstores/faiss/
    - https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.faiss.FAISS.html#langchain_community.vectorstores.faiss.FAISS
"""  # noqa

import os
from typing import Optional

# import faiss
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores.faiss import FAISS
from langchain_community.vectorstores.utils import DistanceStrategy
from langchain.docstore.document import Document as LangchainDocument

current_directory = os.path.dirname(os.path.abspath(__file__))
MAX_CHUNK_SIZE = 512
DEFAULT_EMBEDDING_MODEL = "thenlper/gte-small"
VECTOR_DB_FOLDER_NAME = "current_events_indices"
FAISS_STORE_PATH = os.path.join(current_directory, VECTOR_DB_FOLDER_NAME)
DEFAULT_FAISS_INDEX_NAME = "index"  # name becomes "{name}.faiss"

political_party_to_index_fp_map = {
    "democrat": os.path.join(FAISS_STORE_PATH, "faiss_index_democrat"),
    "republican": os.path.join(FAISS_STORE_PATH, "faiss_index_republican"),  # noqa
    "moderate": os.path.join(FAISS_STORE_PATH, "faiss_index_moderate"),
}

print("Attempting to load embedding model.")
embedding_model = HuggingFaceEmbeddings(
    model_name=DEFAULT_EMBEDDING_MODEL,
    # multi_process=True,
    # model_kwargs={"device": "cuda"},
    # set True for cosine similarity
    encode_kwargs={"normalize_embeddings": True},
)
print("Embedding model loaded.")


def convert_articles_to_documents(
    latest_articles_by_party: dict[str, list[dict]],
) -> dict[str, list[LangchainDocument]]:
    """Convert a list of articles to a list of LangchainDocument objects.

    Combines the article title, content,  and description into one string.
    Takes a maximum chunk size so we don't make our documents too large,
    though this is less a problem since the NewsAPI service only returns
    previews of the articles.
    """
    political_party_to_knowledge_base: dict[str, list[LangchainDocument]] = {
        political_party: [
            LangchainDocument(
                page_content=(
                    article["title"]
                    + " "
                    + article["content"]
                    + " "
                    + article["description"]
                )[:MAX_CHUNK_SIZE],  # noqa
                metadata={
                    "url": article["url"],
                    "publishedAt": article["publishedAt"],
                    "news_outlet_source_id": article["news_outlet_source_id"],
                },
            )
            for article in articles
        ]
        for (political_party, articles) in latest_articles_by_party.items()
    }
    return political_party_to_knowledge_base


def insert_articles_into_vector_store(
    political_party_to_knowledge_base: dict[str, list[LangchainDocument]],
) -> None:
    """Create vector database and indices if they don't exist. Otherwise, loads
    the existing indices and updates them with new articles."""
    for party, articles in political_party_to_knowledge_base.items():
        index_path = political_party_to_index_fp_map[party]
        if not os.path.exists(FAISS_STORE_PATH):
            os.makedirs(FAISS_STORE_PATH)
        # create new vector store + index if necessary
        if not os.path.exists(index_path):
            print(f"Creating FAISS index for {party} at {index_path}...")
            faiss = FAISS.from_documents(
                documents=articles,
                embedding=embedding_model,
                distance_strategy=DistanceStrategy,
            )
            # https://api.python.langchain.com/en/latest/_modules/langchain_community/vectorstores/faiss.html#FAISS.save_local # noqa
            # Can serialize if size is a problem:
            # https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.faiss.FAISS.html#langchain_community.vectorstores.faiss.FAISS.serialize_to_bytes # noqa
            # https://api.python.langchain.com/en/latest/_modules/langchain_community/vectorstores/faiss.html#FAISS.serialize_to_bytes # noqa
            faiss.save_local(
                folder_path=index_path, index_name=DEFAULT_FAISS_INDEX_NAME
            )
            print(f"Created and saved new FAISS index for {party} at {index_path}")  # noqa
            print(
                f"Inserted {len(articles)} articles into the FAISS index for {party} at {index_path}"
            )  # noqa
        # use existing store + index
        else:
            vector_store: FAISS = FAISS.load_local(
                folder_path=index_path,
                embeddings=embedding_model,
                index_name=DEFAULT_FAISS_INDEX_NAME,
                allow_dangerous_deserialization=True,
            )
            print(f"Loaded existing FAISS index for {party} at {index_path}")
            vector_store.add_documents(articles)
            vector_store.save_local(
                folder_path=index_path, index_name=DEFAULT_FAISS_INDEX_NAME
            )
            print(
                f"Inserted {len(articles)} articles into the FAISS index for {party} at {index_path}"
            )  # noqa


def update_vector_store(latest_articles_by_party: dict[str, list[dict]]):
    """Update the vector store with the latest articles."""
    political_party_to_knowledge_base: dict[str, list[LangchainDocument]] = (
        convert_articles_to_documents(latest_articles_by_party)
    )
    insert_articles_into_vector_store(political_party_to_knowledge_base)


def query_political_party_index(
    political_party: str,
    query: str,
    top_n: Optional[int] = 5,
    similarity_distance: Optional[float] = None,
) -> list[dict]:
    """Query the vector store for similar articles."""
    index_path: str = political_party_to_index_fp_map[political_party]
    vector_store: FAISS = FAISS.load_local(
        folder_path=index_path,
        embeddings=embedding_model,
        index_name=DEFAULT_FAISS_INDEX_NAME,
        # avoids value error from deserializing a pickle file.
        allow_dangerous_deserialization=True,
    )

    # in the future, can add filters to search: https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.faiss.FAISS.html#langchain_community.vectorstores.faiss.FAISS.similarity_search_by_vector # noqa
    if similarity_distance is not None:
        raise NotImplementedError(
            "Querying by similarity distance not implemented yet."
        )  # noqa
    else:
        search_results: list[tuple[LangchainDocument], float] = (
            vector_store.similarity_search_with_score(query=query, k=top_n)
        )

    articles: list[dict] = []
    for result in search_results:
        document = result[0]
        score = result[1]
        # Format the result
        article_data = {
            "content": document.page_content,
            "metadata": {
                "url": document.metadata.get("url", "URL not available"),
                "publishedAt": document.metadata.get(
                    "publishedAt", "Date not available"
                ),  # noqa
                "news_outlet_source_id": document.metadata.get(
                    "news_outlet_source_id", "Source ID not available"
                ),  # noqa
                "distance": score,
            },
        }
        articles.append(article_data)
    return articles


def query_vector_store(
    query: str, top_n: Optional[int] = 5, similarity_distance: Optional[float] = None
) -> dict[str, list[dict]]:
    """Queries the vector store for similar articles.

    Given a particular query, searches each index and returns relevant
    articles from each of the indices.
    """
    return {
        political_party: query_political_party_index(
            political_party, query, top_n, similarity_distance
        )  # noqa
        for political_party in political_party_to_index_fp_map.keys()
    }


if __name__ == "__main__":
    test_query = "I can't believe that these protests are happening."
    search_results = query_vector_store(test_query)
