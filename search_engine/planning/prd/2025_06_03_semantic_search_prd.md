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

- [#005] Build basic query classifier
  - Rule-based: classify into [top-k, summarize, unknown]
  - Returns routing instructions (e.g., "use LLM" vs "just filter")

- [#006] Implement router/controller for query dispatch
  - Based on classifier result, call either local filter/sort or semantic LLM pipeline

### Semantic Summarization

- [#007] Write initial prompt templates (YAML or inline)
  - At least 2: summarize_topic, summarize_user_posts

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
