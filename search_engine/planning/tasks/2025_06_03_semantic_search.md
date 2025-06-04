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
- [ ] [#005] Build basic query classifier (2 hrs)
  - [ ] Rule-based classification
  - [ ] Routing instructions

- [ ] [#006] Implement router/controller (2 hrs)
  - [ ] Query dispatch logic
  - [ ] Pipeline integration

### Semantic Summarization
- [ ] [#007] Write initial prompt templates (2 hrs)
  - [ ] Topic summarization
  - [ ] User post summarization

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

## 📊 Status Summary
- Overall Progress: 0%
- Completed Tasks: 0/12
- Estimated Remaining Time: 28 hours
- Timeline: On track for 2-week delivery
