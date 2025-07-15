# Bluesky Post Explorer Backend - Task Plan

**Linear Project ID:** b94f21bf-f11b-4210-a8ef-67904301a8fa
**Project:** Bluesky Post Explorer Backend
**Created:** 2025-07-15

---

## Project Overview
A FastAPI backend for the Bluesky Post Explorer frontend. Provides authentication, post search, and CSV export. Dockerized and ready for extension.

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

## Notes
- Planning, implementation, and review follow the standards in `rules/TASK_PLANNING_AND_PRIORITIZATION.md`.
- All milestones are tracked in Linear and mapped to PRs.
- Next up: MET-17 (Auth Implementation) 