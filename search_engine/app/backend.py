from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import Any
from lib.db.manage_local_data import load_data_from_local_storage

app = FastAPI()


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
