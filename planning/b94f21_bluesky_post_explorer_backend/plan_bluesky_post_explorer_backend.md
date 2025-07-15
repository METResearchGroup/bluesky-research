# Bluesky Post Explorer Backend - Task Plan

**Linear Project ID:** b94f21bf-f11b-4210-a8ef-67904301a8fa
**Project:** Bluesky Post Explorer Backend
**Created:** 2025-07-15

---

## Project Overview
A FastAPI backend for the Bluesky Post Explorer frontend. Provides authentication, post search, and CSV export. Dockerized and ready for extension.

## Task Plan

| Subtask | Deliverable | Dependencies | Effort (hrs) | Linear Issue ID | PR URL |
|---------|-------------|--------------|--------------|-----------------|--------|
| Backend Project Foundation | FastAPI app, Dockerfile, requirements, README | None | 2-3 | [MET-16](https://linear.app/metresearch/issue/MET-16/backend-project-foundation-fastapi-docker-requirements) | [PR #192](https://github.com/METResearchGroup/bluesky-research/pull/192) |
| Auth Implementation | Login page, session, logout, tests | Foundation | 2-3 | [MET-17](https://linear.app/metresearch/issue/MET-17/auth-implementation-login-page-session-logout) | TBA |
| Data Loading Integration | load_preprocessed_posts integration, tests | Foundation | 2-3 | [MET-18](https://linear.app/metresearch/issue/MET-18/data-loading-integration-load-preprocessed-posts) | TBA |
| Search API Endpoint | /posts/search endpoint, docs, tests | Data Loading, Auth | 4-6 | [MET-19](https://linear.app/metresearch/issue/MET-19/search-api-endpoint-postssearch) | TBA |
| CSV Export Endpoint | /posts/export endpoint, tests | Search API, Auth | 3-4 | [MET-20](https://linear.app/metresearch/issue/MET-20/csv-export-endpoint-postsexport) | TBA |
| Filtering Logic | Server-side filtering, tests | Data Loading | 2-3 | [MET-21](https://linear.app/metresearch/issue/MET-21/filtering-logic-server-side) | TBA |
| Error Handling & Validation | Error handling, validation, tests | All above | 2 | [MET-22](https://linear.app/metresearch/issue/MET-22/error-handling-and-validation) | TBA |
| Documentation & CI | README, OpenAPI docs, CI | All above | 2 | [MET-23](https://linear.app/metresearch/issue/MET-23/documentation-and-ci-integration) | TBA |
| Extensibility Prep | ML filter stubs, docs | All above | 1 | [MET-24](https://linear.app/metresearch/issue/MET-24/extensibility-prep-ml-powered-filters) | TBA |

---

## Notes
- Planning, implementation, and review follow the standards in `rules/TASK_PLANNING_AND_PRIORITIZATION.md`.
- All milestones are tracked in Linear and mapped to PRs.
- Next up: MET-17 (Auth Implementation) 