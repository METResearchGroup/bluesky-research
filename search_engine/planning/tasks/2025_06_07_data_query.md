# Bluesky Data Access App Demo - Daily Task Tracking

## 📝 Overview
This document tracks daily progress on the Streamlit Data Access App Demo as defined in [2025_06_07_data_query_prd.md](../prd/2025_06_07_data_query_prd.md). The goal is to deliver a fully interactive, demo-ready data-access interface for Bluesky social science research within 2 weeks. All implementation code will be developed in the new `search_engine/app_v2` directory.

## 📋 Task Checklist

### UI Components
- [ ] [#001] Filter Builder Panel (4 hrs)
  - [ ] Accordion sections for: Temporal, Content, Sentiment, Political, User, Engagement, Network
  - [ ] Chips display for active filters
  - [ ] Selecting controls displays chips in header
  - [ ] Dependencies: sample JSON schema for filters

- [ ] [#002] Query Preview & Sample Data (3 hrs)
  - [ ] Static sample tables (posts, users, reactions)
  - [ ] Display top 5 rows matching filter state
  - [ ] Changing filter toggles sample rows accordingly

- [ ] [#003] Visualization Quick‑Look (3 hrs)
  - [ ] Mini line chart showing daily count (mock data)
  - [ ] Use Streamlit's `st.line_chart` with sample time series
  - [ ] Reacts to date‑range filter

- [ ] [#004] Export & Templates Panel (3 hrs)
  - [ ] Show estimated record count & data footprint (hard‑coded logic)
  - [ ] Export button triggers download of sample CSV/Parquet
  - [ ] Saved‑query dropdown loads predefined filter sets
  - [ ] Template selection applies filters; Export downloads file

### Static Assets & Data
- [ ] [#005] Prepare Sample CSV/Parquet Files (2 hrs)
  - [ ] Create 3 sample files: `posts.csv`, `users.csv`, `engagement.csv`
  - [ ] Ensure variety of valence, toxicity, slant, timestamps
  - [ ] Files load into demo and power preview/export

- [ ] [#006] JSON Schema for Saved Queries (1 hr)
  - [ ] Define 3 templates (e.g. toxic‑posts, cohort‑engagement, topic‑time)
  - [ ] Frontend can parse and apply template filters

### Integration & Polish
- [ ] [#007] Wire up Streamlit Layout & Navigation (4 hrs)
  - [ ] Combine panels into cohesive layout: sidebar vs main vs bottom
  - [ ] Include app title, description, help tooltips
  - [ ] Navigable demo with clear labels

- [ ] [#008] User Onboarding & Tooltips (2 hrs)
  - [ ] Add contextual tooltips for each filter group
  - [ ] Include a "Getting Started" collapsible help pane
  - [ ] Tooltips appear on hover or click

- [ ] [#009] Documentation & README (2 hrs)
  - [ ] Write usage guide, sample queries, and file naming convention
  - [ ] Instructions for running `streamlit run app.py`
  - [ ] New user can launch demo without support

## 📝 Progress Notes

### Day 1 (2025-06-07)
- Project planning and task tracking file created.
- Next: Begin UI component scaffolding (#001)

## 📊 Status Summary
- Overall Progress: 0%
- Completed Tasks: 0/9
- Estimated Remaining Time: 24 hours
- Timeline: On track for 2-week delivery 