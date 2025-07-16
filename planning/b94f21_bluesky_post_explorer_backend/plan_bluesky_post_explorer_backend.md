# Bluesky Post Explorer Backend - Task Plan

**Linear Project ID:** b94f21bf-f11b-4210-a8ef-67904301a8fa
**Project:** Bluesky Post Explorer Backend
**Created:** 2025-07-15

---

## Project Overview
A FastAPI backend for the Bluesky Post Explorer frontend. Provides authentication, post search, and CSV export. Dockerized and ready for extension.

Researchers need to efficiently query, filter, and export Bluesky posts for analysis. This backend enables those capabilities, serving as the data and API layer for the Bluesky Post Explorer frontend.

## Objective & Success Criteria
- Deliver a working MVP backend that exposes API endpoints for querying, filtering, and exporting Bluesky posts.
- Ensure seamless integration with the already-deployed frontend (Purcell).
- Backend must be deployable on a third-party provider (Render) with both compute and data storage on the same server for MVP simplicity.
- Users (researchers) must be able to query the backend via the API and receive correct results.
- No analytics, ML-powered filters, or dashboards are required for v1.

## Scope & Deliverables
**In Scope:**
- API endpoints for authentication, post search, and CSV export
- Dockerized deployment
- Integration with the existing frontend
- Deployment to Render with local data storage

**Out of Scope:**
- Analytics, ML-powered filters, dashboards
- S3 or cloud object storage (local storage only for MVP)
- Pagination or large-scale data optimizations (not needed for MVP)

## Deployment Planning
- The backend will be deployed to Render.
- For MVP, both compute and data storage will reside on the same server (Render persistent disk).
- S3 or distributed storage is not required at this stage.
- Final provider selection is Render for MVP; will evaluate future migration as needed.

## Task Plan

| Subtask | Deliverable | Dependencies | Effort (hrs) | Priority Score | Linear Issue ID | Linear Issue Identifier | PR URL |
|---------|-------------|--------------|--------------|---------------|-----------------|------------------------|--------|
| Backend Project Foundation | FastAPI app, Dockerfile, requirements, README | None | 2-3 | 5 | [MET-16](https://linear.app/metresearch/issue/MET-16/backend-project-foundation-fastapi-docker-requirements) | MET-16 | [PR #192](https://github.com/METResearchGroup/bluesky-research/pull/192) |
| Auth Implementation | Login page, session, logout, tests | Foundation | 2-3 | 5 | [MET-17](https://linear.app/metresearch/issue/MET-17/auth-implementation-login-page-session-logout) | MET-17 | TBA |
| Data Loading Integration | load_preprocessed_posts integration, tests | Foundation | 2-3 | 5 | [MET-18](https://linear.app/metresearch/issue/MET-18/data-loading-integration-load-preprocessed-posts) | MET-18 | TBA |
| Search API Endpoint | /posts/search endpoint, docs, tests | Data Loading, Auth | 4-6 | 5 | [MET-19](https://linear.app/metresearch/issue/MET-19/search-api-endpoint-postssearch) | MET-19 | TBA |
| CSV Export Endpoint | /posts/export endpoint, tests | Search API, Auth | 3-4 | 4 | [MET-20](https://linear.app/metresearch/issue/MET-20/csv-export-endpoint-postsexport) | MET-20 | TBA |
| Filtering Logic | Server-side filtering, tests | Data Loading | 2-3 | 5 | [MET-21](https://linear.app/metresearch/issue/MET-21/filtering-logic-server-side) | MET-21 | TBA |
| Error Handling & Validation | Error handling, validation, tests | All above | 2 | 4 | [MET-22](https://linear.app/metresearch/issue/MET-22/error-handling-and-validation) | MET-22 | TBA |
| Documentation & CI | README, OpenAPI docs, CI | All above | 2 | 4 | [MET-23](https://linear.app/metresearch/issue/MET-23/documentation-and-ci-integration) | MET-23 | TBA |
| Extensibility Prep | ML filter stubs, docs | All above | 1 | 3 | [MET-24](https://linear.app/metresearch/issue/MET-24/extensibility-prep-ml-powered-filters) | MET-24 | TBA |

---

## ⚙️ FastAPI + DuckDB + Parquet on Render: System Design & Setup Plan

This setup enables you to:

- Serve a backend API using FastAPI
- Store partitioned `.parquet` files on a Render-mounted persistent disk
- Run analytical queries over those files using DuckDB
- Upload new `.parquet` partitions via API
- Deploy everything via a single Docker container

---

### 🧱 Architecture Overview

```
                ┌─────────────────────┐
                │   Vercel Frontend   │
                └────────┬────────────┘
                         │
                         ▼
                ┌─────────────────────┐
                │  FastAPI on Render   │
                │   (Dockerized App)   │
                └────────┬────────────┘
                         │
        ┌─────────────────────────────────────┐
        │ Persistent Disk Mounted at /data     │
        │ - /data/date=2024-07-15/posts.parquet│
        │ - /data/date=2024-07-16/posts.parquet│
        └─────────────────────────────────────┘
                         │
                         ▼
                ┌─────────────────────┐
                │     DuckDB Engine    │
                │   Reads from /data   │
                └─────────────────────┘
```

---

### 🗂️ Project Structure
project/
├── app/
│ ├── main.py # FastAPI app
│ └── utils.py # DuckDB queries, upload helpers
├── Dockerfile # FastAPI + DuckDB container
└── render.yaml # Render service definition with disk mount

---

### render.yaml
services:
  - type: web
    name: parquet-demo-backend
    env: docker
    plan: starter  # upgrade to standard for disk support
    disk:
      name: parquet-storage
      mountPath: /data
      sizeGB: 10
    autoDeploy: true

✅ Deployment Steps
✅ Push code to GitHub

✅ Create new Render Web Service:

Select your GitHub repo

Choose Docker environment

Attach a Persistent Disk at /data

✅ Test endpoints:

POST /upload?date=2024-07-16

GET /query

📈 Future Migration Plan (for >100GB)
Change	Next Step
Storage	Migrate /data → s3://my-bucket/...
DuckDB I/O	Add httpfs plugin and read from S3
Compute Scaling	Move queries to Modal, AWS Lambda, or ECS
Auth & Data Sharing	Add Supabase for Auth + signed URLs

🧠 Tips
Keep files partitioned in the same structure: date=YYYY-MM-DD/

Use ZSTD-compressed .parquet to reduce disk usage

Use DuckDB’s SQL for aggregation, filtering, sampling

curl -X POST "https://your-render-url/upload?date=2024-07-16" \
     -F "file=@posts.parquet"

---

## Notes
- Planning, implementation, and review follow the standards in `rules/TASK_PLANNING_AND_PRIORITIZATION.md`.
- All milestones are tracked in Linear and mapped to PRs.
- Next up: MET-17 (Auth Implementation) 