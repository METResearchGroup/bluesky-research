# MVP PRD: Semantic Search Engine (Weeks 1–2)

## 🧩 Objective

Deliver a minimum viable product (MVP) of the semantic search engine that allows a user to:

- Submit a freeform query in a Streamlit UI
- Load Bluesky data (1 week of posts/follows) via DuckDB
- Answer structured or summarization queries using a semantic LLM layer

This MVP should be modular and ready for orchestrator integration in future phases.

## 📌 Scope

### ✅ In-Scope

- Streamlit UI (text input, results display)
- Query sanitization and validation
- DuckDB parquet reader for static 1-week data
- Simple query routing (e.g., rule-based: summary vs. top-k)
- Prompt templates for supported query types
- OpenAI API integration for summarization tasks
- Answer composer: text + optional tabular result
- Basic error handling for failed queries or invalid input

### ❌ Out-of-Scope (Deferred to Later Phases)

- Orchestrator DAG
- Vector search or RAG
- Prompt registry with versions
- Caching, retries, rate limiting
- Advanced observability or fallback logic

## 🎫 Task Breakdown (MVP Tickets)

Estimated: 2–3 hrs each; intended to be tracked individually in an issue tracker.

### UI + Input Pipeline

- [#001] Create Streamlit UI with text input and results box
  - Basic layout with form.
  - Results area supports markdown and expandable table.
  - Show dummy inputs and outputs. No I/O or external API calls yet.
  - Basic submission logic - user writes a question, then app returns a generic
  dummy response.
  - Underneath the search bar, the app suggests example queries that are popular.
  - Basic title, relevant to the project.
  - Basic subtitle, relevant to the project.
  - Basic description, relevant to the project.
  - Stakeholders: nontechnical social scientists, interested in easily being able
  to access technical data using a search interface.

- [#002] Add input validation + sanitation
  - Filter for empty inputs, SQL injection-like text, profanity, length caps
  - Optional: basic anti-spam guard (e.g., rate limit per session)

### Data Handling

- [#003] Load parquet files with DuckDB for a fixed date range
  - Start development of the FastAPI backend to support the UI. The FastAPI backend
  should expose a simple interface that the Streamlit app can query. Start with
  creating one simple endpoint, called "fetch-results".
  - Read from local .parquet files (e.g., posts/2024-06-01/, etc.). Use the helper
  tooling in "manage_local_data.py", specifically the function "load_data_from_local_storage". Let's just return all posts from the date
  range "2024-11-10" to "2024-11-21". See "get_details_of_posts_liked_by_study_users"
  from the "load_users_engaged_with_by_study_users.py" file for instructions on how
  to load likes, and then replace record type with "post".
  - Return as pandas DataFrame, return the top 10 posts. Also print out how many
  results there originally were in the query (e.g., "Found XXX results related to your query").

- [#004] Preprocess post data after load
  - Lowercasing, timestamp normalization, drop nulls
  - Add computed fields like engagement_score = likes + reposts

### Routing Logic

- [#005] Build basic intent classifier
  - Classify into [top-k, summarize, unknown]. Uses an LLM call to
  dynamically route the query. Create a query to do this routing, then interpolate
  with the actual text of the query itself. Use run_query, using the "GPT-4o mini"
  model, with custom kwargs, changing the current kwargs to use a Pydantic model
  to constrain the format. Encapsulate all of this into an "intent_classifier.py"
  with a "classify_query_intent" function that returns the intent and also the reason for the intent (ask the model to give a reason). The Pydantic model should have two fields, "intent" (top-k, summarize, unknown), and "reason".
  - Connect the intent classifier with the FastAPI backend. Create a new endpoint,
  'get-query-intent'. Upon submitting a query in the UI, have the query be sent
  to the intent classifier. Underneath the dummy results, put a "Classified intent" section that returns the output of the intent classifier and then the reason.

- [#006] Implement router/controller for query dispatch
  - Create a "router.py" file. This file will take the output of the intent
  classifier and then, based on the intent, do the routing logic.
  - Create a "dataloader.py" file. Expose a simple interface, "load_data", that takes as input the source ("keyword", "query_engine", or "rag"), that calls a function ("get_data_from_keyword", 'get_data_from_query_engine", "get_data_from_rag"), and then "kwargs", keyword parameters that are passed to the respective loader function.
  - Have each function return the same dummy result for now (loading all posts from the same date interval as what is in the current app.py logic). Have each function print out something like "Retrieving data from keyword", for example.
  - Expose the router and dataloader in FastAPI. For the router, create an endpoint, "route-query", and for the dataloader, create an endpoint, "get-data". Have each of these endpoints take payloads, corresponding to the details needed for their functionality.
  - Connect all of this to the frontend UI. Have the intent classifier trigger the router, then have the router trigger the dataloader, all via FastAPI. Then take the results and display them in the app. Also display in the UI (1) the classified intent, and (2) the source for the dataloader.

### Semantic Summarization

- [#007] Create the first pass of the "semantic_summarizer" service.
  - Create a "semantic_summarizer.py" file. This file will take the output of the router and "understand" it (meaning, taking the output and (1) creating a human-friendly summary) and (2) deciding what visuals to create, if any.
  - Expose an interface, "summarize_router_results", that takes the input and returns a dictionary. This will be keyed on the asset type (text response, graph visual, etc). For now, have it manually return the following:

  ```json
  {
    "text": """
    The total number of posts per day is:
    
    [YYYY-MM-DD]: <count>
    [YYYY-MM-DD]: <count>
    [YYYY-MM-DD]: <count>
    [YYYY-MM-DD]: <count>
    [YYYY-MM-DD]: <count>
    """, // actually calculate these.
    "graph": [
      {
        "type": "line",
        "kwargs": {
          "transform": {
            "groupby": True,
            "groupby_col": "partition_date",
            "agg_func": "count",
            "agg_col": "counts"
          },
          "graph": {
            "col_x": "partition_date",
            "xlabel": "Date",
            "col_y": "counts",
            "ylabel": "Date",
          }
        }
      },
      {
        "type": "bar",
        "kwargs": {
          "transform": {
            "groupby": True,
            "groupby_col": "partition_date",
            "agg_func": "count",
            "agg_col": "counts"
          },
          "graph": {
            "col_x": "partition_date",
            "xlabel": "Date",
            "col_y": "counts",
            "ylabel": "Date",
          }
        }
      }
    ]
  }
  ```

  - Create an "answer_composer.py" that takes the output of the "semantic_summarizer.py" logic and figures out the best presentation. It should do the following:
    - For any visualization code, actually generate the graph as a .png file and
     save it to a temp file for the UI to access later on. Save the paths to each of the .png files.

    The visualization kwargs should correspond to something like the following code:

    ```python
    import pandas as pd
    import matplotlib.pyplot as plt

    # Ensure partition_date is in datetime format (optional but recommended for plotting)
    df['partition_date'] = pd.to_datetime(df['partition_date'])

    # -------- Transform step (shared by both plots) --------
    # Perform groupby + aggregation
    grouped_df = df.groupby('partition_date').size().reset_index(name='counts')
    # At this point:
    # grouped_df has two columns: 'partition_date' and 'counts'

    # -------- Line Plot --------
    plt.figure(figsize=(10, 5))
    plt.plot(grouped_df['partition_date'], grouped_df['counts'])
    plt.xlabel('Date')
    plt.ylabel('Date')  # You may want to adjust this, 'Date' for ylabel may not be semantically accurate
    plt.title('Line Plot: Count of Rows per Date')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # -------- Bar Plot --------
    plt.figure(figsize=(10, 5))
    plt.bar(grouped_df['partition_date'], grouped_df['counts'])
    plt.xlabel('Date')
    plt.ylabel('Date')  # Same note as above
    plt.title('Bar Plot: Count of Rows per Date')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    ```

    - Return a dictionary result, in the form:

    ```python
    {
      "text": "<text>",
      "df": <the actual dataframe>,
      "visuals": [
        {"type": "line", "path": "<path to asset>"},
        {"type": "bar", "path": "<path to asset>"},
      ]
    }
    ```

  - Expose the "semantic_summarizer.py" and "answer_composer.py" via the FastAPI backend. Use the routes "summarize-results" and "compose-answer" respectively.
  - This should be triggered in the following way:
    - The router's results should be passed to "summarize-results".
    - The summarizer's results should be passed to "compose-answer".
    - The UI, when submitting the query, should only be managing the results
    of "compose-answer".
  - The frontend is given the results of the answer composer. It should handle it in this way:
    - Display the "text" field as "[AI-generated response]: <response>"
    - Display the .head() of the dataframe in "df". Also print out in grey text italics the number of total rows in df, something like "Found <XYZ> total results".
    - For each of the visuals in "visuals", load the asset picture and display it.

- [#008] Integrate OpenAI API for semantic understanding
  - Call LLM with prompt + formatted tabular data
  - Capture output, retry on error once, return JSON/text

### Output Composition

- [#009] Implement answer composer
  - Combine LLM summary + optional data preview (e.g., top 3 rows)
  - Ensure formatting is user-readable in Streamlit

- [#010] Add error display + safe fallback response
  - E.g., "Sorry, I couldn't process that question. Try rephrasing."
  - Catch DuckDB, LLM, or classifier errors

### Integration + Polish

- [#011] Refactor modules into services/ or pipelines/
  - Basic code hygiene, single entrypoint

- [#012] Write README + quickstart instructions
  - Includes setup, how to run, example queries

## 🔁 Example Supported Queries for MVP

- "What are the most liked posts this week?"
- "Summarize what people are saying about climate."
- "What did @user123 post about the election?"

## 🧪 Acceptance Criteria

- Users can submit a query and get a response in under 5s
- Data loaded from DuckDB, processed, and optionally summarized
- Summaries are coherent and relevant 90%+ of time (subjective)
- App does not crash on malformed queries
- Code is modular and ready for orchestration integration in Phase 2

## ⏳ Estimated Time (1 Engineer)

~25–30 hours over 1–2 weeks, assuming intermediate-level fluency with Streamlit, DuckDB, OpenAI API, and pandas.

## 🧾 Deliverables

- ✅ Streamlit app with UI and end-to-end working query flow
- ✅ Fixed-week data source + filters
- ✅ LLM-backed summarizer
- ✅ Demo-ready UI
- ✅ Documentation + example queries
- 🚫 No orchestration, retries, fallback, or RAG yet
