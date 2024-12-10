"""Retrieves relevant documents from the vector database for a given query."""

from typing import Optional

from langchain.docstore.document import Document as LangchainDocument

from services.classify_civic.update_current_events.vectorize_articles import (
    get_or_create_vector_database,
)

TOP_K = 5
vector_db = get_or_create_vector_database()


def get_documents_for_query(
    query: str, k: Optional[int] = TOP_K
) -> list[LangchainDocument]:
    top_k_results = vector_db.similarity_search(query=query, k=k)
    return top_k_results


if __name__ == "__main__":
    user_query = "Can you believe what's happening with Democrats and IVF access?"  # noqa
    top_k_results = get_documents_for_query(user_query)
    print(f"Query: {user_query}")
    print(f"Top {TOP_K} results:")
    for result in top_k_results:
        print("-" * 10)
        print(f"Document ID: {result.metadata['id']}")
        print(f"Document URI: {result.metadata['nytimes_uri']}")
        print(f"Document Content: {result.page_content}")
    print("-" * 10)
    print("Finished searching the vector database.")
