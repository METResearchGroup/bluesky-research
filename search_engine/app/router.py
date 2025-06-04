from typing import Any, Dict
from search_engine.app.dataloader import load_data


class Router:
    """
    Router for dispatching queries based on classified intent.
    """

    def __init__(self) -> None:
        pass

    def route_query(self, intent: str, query: str, **kwargs) -> Dict[str, Any]:
        """
        Route the query to the appropriate data loader based on intent.

        Args:
            intent: The classified intent ('top-k', 'summarize', 'unknown')
            query: The user query string
            **kwargs: Additional parameters for the loader
        Returns:
            A dictionary with the loaded data and metadata
        """
        if intent == "top-k":
            print("Routing to get_data_from_keyword")
            return load_data(source="keyword", query=query, **kwargs)
        elif intent == "summarize":
            print("Routing to get_data_from_query_engine")
            return load_data(source="query_engine", query=query, **kwargs)
        else:
            print("Routing to get_data_from_rag (unknown intent)")
            return load_data(source="rag", query=query, **kwargs)


def route_query(intent: str, query: str, **kwargs) -> Dict[str, Any]:
    """
    Functional interface for routing a query based on intent.
    """
    router = Router()
    return router.route_query(intent, query, **kwargs)
