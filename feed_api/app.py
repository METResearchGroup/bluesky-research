from typing import Union

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.get("/user/{bluesky_user_handle}/feed")
def get_latest_feed_for_user(bluesky_user_handle: str):
    return {"bluesky_user_handle": bluesky_user_handle, "feed": []}
