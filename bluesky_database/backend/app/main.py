from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Bluesky Post Explorer Backend")


@app.get("/", response_class=JSONResponse)
def root() -> dict:
    """Root endpoint for health check and welcome message."""
    return {"message": "Welcome to the Bluesky Post Explorer Backend!"}
