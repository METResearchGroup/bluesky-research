# Semantic Search Engine MVP - TODO

## UI + Input Pipeline
- [x] [#001] Create Streamlit UI with text input and results box
- [x] [#002] Add input validation + sanitation

## Data Handling
- [x] [#003] Load parquet files with DuckDB
- [x] [#004] Preprocess post data

## Routing Logic
- [x] [#005] Build basic query intent classifier
- [ ] [#006] Implement router/controller
    - [x] Create a `router.py` file. This file will take the output of the intent classifier and then, based on the intent, do the routing logic.
    - [x] Create a `dataloader.py` file. Expose a simple interface, `load_data`, that takes as input the source ("keyword", "query_engine", or "rag"), that calls a function ("get_data_from_keyword", 'get_data_from_query_engine", "get_data_from_rag"), and then "kwargs", keyword parameters that are passed to the respective loader function.
    - [x] Have each function return the same dummy result for now (loading all posts from the same date interval as what is in the current app.py logic). Have each function print out something like "Retrieving data from keyword", for example.
    - [x] Expose the router and dataloader in FastAPI. For the router, create an endpoint, "route-query", and for the dataloader, create an endpoint, "get-data". Have each of these endpoints take payloads, corresponding to the details needed for their functionality.
    - [x] Connect all of this to the frontend UI. Have the intent classifier trigger the router, then have the router trigger the dataloader, all via FastAPI. Then take the results and display them in the app. Also display in the UI (1) the classified intent, and (2) the source for the dataloader.

## Semantic Summarization
- [ ] [#007] Write initial prompt templates
    - [ ] Topic summarization
    - [ ] User post summarization
- [ ] [#008] Integrate OpenAI API
    - [ ] LLM integration
    - [ ] Error handling

## Output Composition
- [ ] [#009] Implement answer composer
    - [ ] Result formatting
    - [ ] Data preview
- [ ] [#010] Add error display
    - [ ] Error handling
    - [ ] Fallback responses

## Integration + Polish
- [ ] [#011] Refactor modules
    - [ ] Code organization
    - [ ] Entrypoint setup
- [ ] [#012] Write README + quickstart
    - [ ] Setup instructions
    - [ ] Example queries 