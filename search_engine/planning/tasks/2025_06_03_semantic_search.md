# Semantic Search Engine - Daily Task Tracking

## 📌 Overview
This document tracks daily progress on implementing the semantic search engine MVP as defined in [2025_06_03_semantic_search_prd.md](../prd/2025_06_03_semantic_search_prd.md). The goal is to deliver a working Streamlit-based search interface for Bluesky data within 2 weeks.

## 📋 Task Checklist

### UI + Input Pipeline
- [x] [#001] Create Streamlit UI with text input and results box (3 hrs)
  - [x] Basic layout with form
  - [x] Results area with markdown and expandable table
  - [x] Dummy I/O implementation
  - [x] Example query suggestions
  - [x] Title, subtitle, and description
  - [x] Stakeholder-focused design

- [ ] [#002] Add input validation + sanitation (2 hrs)
  - [ ] Filter for empty inputs, SQL injection-like text, profanity, length caps
  - [ ] Optional: basic anti-spam guard (e.g., rate limit per session)

### Data Handling
- [x] [#003] Load parquet files with DuckDB (2 hrs)
  - [x] Start development of the FastAPI backend to support the UI. The FastAPI backend should expose a simple interface that the Streamlit app can query. Start with creating one simple endpoint, called "fetch-results".
  - [x] Read from local .parquet files (e.g., posts/2024-06-01/, etc.). Use the helper
  tooling in "manage_local_data.py", specifically the function "load_data_from_local_storage". Let's just return all posts from the date
  range "2024-11-10" to "2024-11-21". See "get_details_of_posts_liked_by_study_users"
  from the "load_users_engaged_with_by_study_users.py" file for instructions on how
  to load likes, and then replace record type with "post".
  - [x] Return as pandas DataFrame, return the top 10 posts. Also print out how many
  results there originally were in the query (e.g., "Found XXX results related to your query").

- [ ] [#004] Preprocess post data (2 hrs)
  - [ ] Text normalization
  - [ ] Computed fields

### Routing Logic
- [x] [#005] Build basic query intent classifier (2 hrs)
  - [x] Classify into [top-k, summarize, unknown].
  - [x] Uses an LLM call to dynamically route the query. Create a query to do this routing, then interpolate with the actual text of the query itself. Use run_query, using the "GPT-4o mini" model, with custom kwargs, changing the current kwargs to use a Pydantic model to constrain the format. The Pydantic model should have two fields, "intent" (top-k, summarize, unknown), and "reason".
  - [x] Encapsulate all of this into an "intent_classifier.py" with a "classify_query_intent" function that returns the intent and also the reason (ask the model to give a reason).
  - [x] Connect the intent classifier with the FastAPI backend. Create a new endpoint,
  'get-query-intent'. Upon submitting a query in the UI, have the query be sent
  to the intent classifier. Underneath the dummy results, put a "Classified intent" section that returns the output of the intent classifier and then the reason.

- [x] [#006] Implement router/controller (2 hrs)
  - [x] Create a "router.py" file. This file will take the output of the intent
  classifier and then, based on the intent, do the routing logic.
  - [x] Create a "dataloader.py" file. Expose a simple interface, "load_data", that takes as input the source ("keyword", "query_engine", or "rag"), that calls a function ("get_data_from_keyword", 'get_data_from_query_engine", "get_data_from_rag"), and then "kwargs", keyword parameters that are passed to the respective loader function.
  - [x] Have each function return the same dummy result for now (loading all posts from the same date interval as what is in the current app.py logic). Have each function print out something like "Retrieving data from keyword", for example.
  - [x] Expose the router and dataloader in FastAPI. For the router, create an endpoint, "route-query", and for the dataloader, create an endpoint, "get-data". Have each of these endpoints take payloads, corresponding to the details needed for their functionality.
  - [x] Connect all of this to the frontend UI. Have the intent classifier trigger the router, then have the router trigger the dataloader, all via FastAPI. Then take the results and display them in the app. Also display in the UI (1) the classified intent, and (2) the source for the dataloader.

### Semantic Summarization

- [x] [#007] Create the first pass of the "semantic_summarizer" service.

  - [x] Create a "semantic_summarizer.py" file. This file will take the output of the router and "understand" it (meaning, taking the output and (1) creating a human-friendly summary) and (2) deciding what visuals to create, if any.
  - [x] Expose an interface, "summarize_router_results", that takes the input and returns a dictionary. This will be keyed on the asset type (text response, graph visual, etc). For now, have it manually return the following:

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

  - [x] Create an "answer_composer.py" that takes the output of the "semantic_summarizer.py" logic and figures out the best presentation. It should do the following:
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

  - [x] Expose the "semantic_summarizer.py" and "answer_composer.py" via the FastAPI backend. Use the routes "summarize-results" and "compose-answer" respectively.
  - [x] This should be triggered in the following way:
    - The router's results should be passed to "summarize-results".
    - The summarizer's results should be passed to "compose-answer".
    - The UI, when submitting the query, should only be managing the results
    of "compose-answer".
  - [x] The frontend is given the results of the answer composer. It should handle it in this way:
    - Display the "text" field as "[AI-generated response]: <response>"
    - Display the .head() of the dataframe in "df". Also print out in grey text italics the number of total rows in df, something like "Found <XYZ> total results".
    - For each of the visuals in "visuals", load the asset picture and display it.

- [ ] [#008] Integrate OpenAI API (3 hrs)
  - [ ] LLM integration
  - [ ] Error handling

### Output Composition
- [ ] [#009] Implement answer composer (2 hrs)
  - [ ] Result formatting
  - [ ] Data preview

- [ ] [#010] Add error display (2 hrs)
  - [ ] Error handling
  - [ ] Fallback responses

### Integration + Polish
- [ ] [#011] Refactor modules (2 hrs)
  - [ ] Code organization
  - [ ] Entrypoint setup

- [ ] [#012] Write README + quickstart (2 hrs)
  - [ ] Setup instructions
  - [ ] Example queries

## 📝 Progress Notes

### Day 1 (2025-06-03)
- Initial project setup
- Created task tracking document
- Set up development environment
- Next: Begin UI implementation (#001)

### Day 2 (2025-06-04)
- Completed backend and frontend integration for intent classification, routing, and data loading.
- The UI now displays the classified intent and data source for each query.
- All routing logic and FastAPI endpoints are fully functional and tested.
- Next: Begin semantic summarization prompt template implementation (#007)

## 📊 Status Summary
- Overall Progress: 0%
- Completed Tasks: 0/12
- Estimated Remaining Time: 28 hours
- Timeline: On track for 2-week delivery
