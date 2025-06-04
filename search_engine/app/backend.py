from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi import Body
from pydantic import BaseModel
from typing import Any, Dict
from lib.db.manage_local_data import load_data_from_local_storage
from search_engine.app.intent_classifier import classify_query_intent
from search_engine.app.router import route_query
from search_engine.app.dataloader import load_data
from search_engine.app.semantic_summarizer import summarize_router_results
from search_engine.app.answer_composer import compose_answer

app = FastAPI()


class RouteQueryRequest(BaseModel):
    intent: str
    query: str
    kwargs: Dict[str, Any] = {}


class GetDataRequest(BaseModel):
    source: str
    query: str
    kwargs: Dict[str, Any] = {}


@app.get("/fetch-results")
def fetch_results() -> Any:
    """
    Fetch the top 10 posts from local parquet files for the date range 2024-11-10 to 2024-11-21.
    Returns a JSON response with:
      - 'posts': list of top 10 posts (as dicts)
      - 'total_count': total number of posts found
    """
    df = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        export_format="parquet",
        start_partition_date="2024-11-10",
        end_partition_date="2024-11-21",
    )
    total_count = len(df)
    print(f"Found {total_count} posts")
    posts = df.head(10).to_dict(orient="records")
    return JSONResponse(content={"posts": posts, "total_count": total_count})


@app.get("/get-query-intent")
def get_query_intent(query: str = Query(..., description="User query")) -> Any:
    """
    Classify the intent of a user query and return the intent and reason.
    """
    result = classify_query_intent(query)
    return JSONResponse(content=result)


@app.post("/route-query")
def route_query_endpoint(payload: RouteQueryRequest = Body(...)) -> Any:
    """
    Route a query based on classified intent and return the result.
    """
    result = route_query(payload.intent, payload.query, **payload.kwargs)
    return JSONResponse(content=result)


@app.post("/get-data")
def get_data_endpoint(payload: GetDataRequest = Body(...)) -> Any:
    """
    Load data from the specified source and return the result.
    """
    result = load_data(payload.source, payload.query, **payload.kwargs)
    return JSONResponse(content=result)


@app.post("/summarize-results")
def summarize_results_endpoint(payload: Dict[str, Any] = Body(...)) -> Any:
    """
    Summarize the router results. Expects a payload with 'posts' as a list of dicts.
    Returns a dict with 'text' and 'graph'.
    """
    import pandas as pd

    df = pd.DataFrame(payload["posts"])
    result = summarize_router_results(df)
    return JSONResponse(content=result)


@app.post("/compose-answer")
def compose_answer_endpoint(payload: Dict[str, Any] = Body(...)) -> Any:
    """
    Compose the final answer. Expects a payload with 'summarizer_output' and 'posts'.
    Returns a dict with 'text', 'df', and 'visuals'.
    """
    import pandas as pd

    summarizer_output = payload["summarizer_output"]
    df = pd.DataFrame(payload["posts"])
    result = compose_answer(summarizer_output, df)
    return JSONResponse(content=result)
