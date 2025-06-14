# Semantic Search Engine for Bluesky Data — PRD

## 📌 Overview

This document outlines the product requirements for building a semantic search engine over Bluesky social media data. The goal is to support freeform natural language queries that are grounded in structured `.parquet` data, with semantic understanding powered by LLMs. The system should be modular, scalable, and demonstrate production-quality engineering practices even in MVP.

## 🎯 Goals

* Build an interactive semantic search demo for Bluesky data.
* Support a set of natural language queries such as summarization, ranking, and user-specific lookups.
* Compose results using a semantic layer powered by an LLM (e.g., OpenAI).
* Incorporate extensible modules for orchestration, observability, fallbacks, and cost monitoring.
* Deliver value in iterative 2-week milestones with a working demo and changelog.

---

## 🧱 Architecture Overview

```
User
 │
 ▼
UI Layer (Streamlit)
 └──> Accepts query input, renders results, displays errors
 │
 ▼
Gateway
 └──> Input sanitization, rate limiting, experiment tagging
 │
 ▼
Query Classifier
 └──> Determines type of query (summary, top-k, trend, etc.)
 │
 ▼
Orchestrator (Execution DAG)
 └──> Routes tasks to:
     ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐
     │  Data Load │→│ Transform   │→│ Vector/RAG  │→│ Semantic LLM│
     └────────────┘  └────────────┘  └────────────┘  └────────────┘
 │
 ▼
Answer Composer + Postprocessing
 └──> Validates, cleans, formats output
 │
 ▼
Presentation
 └──> Final display to user
```

## 🔩 Supporting Services

* **Prompt Registry**: YAML/JSON store of prompt templates and versions.
* **LLM Fallback Layer**: Contract enforcement, retries, and backup models.
* **Evaluator/Scorer**: Automated scoring of LLM outputs.
* **Experimentation Framework**: A/B testing of prompts, routing logic.
* **Cost Monitor**: Tracks token usage, latency, budget enforcement.
* **Rate Limiter**: Input throttling by session/IP.
* **Data Catalog / Schema Registry**: Metadata for fields, datatypes, usage.
* **Model Registry**: Tracks prompt version, LLM used, and retrieval strategy.
* **Observability + Logging**: Captures execution time, retries, and output logs.
* **Caching Layer**: DuckDB and LLM response caching.

---

## 📆 Milestones & Timeline

| Phase                           | Timeline    | Features                                                      |
| ------------------------------- | ----------- | ------------------------------------------------------------- |
| **1. MVP**                      | Weeks 1–2   | Streamlit UI, DuckDB loading, semantic summarization          |
| **2. Orchestration**            | Weeks 3–4   | Modular execution DAG, prompt registry, observability         |
| **3. Semantic Index**           | Weeks 5–6   | FAISS/Qdrant-based RAG engine                                 |
| **4. Robustness**               | Weeks 7–8   | Fallback LLMs, retry logic, caching, rate limiting            |
| **5. Evaluation**               | Weeks 9–10  | Scoring framework, cost monitoring                            |
| **6. Polish & Dynamic Loading** | Weeks 11–12 | UI improvements, schema-aware querying, dynamic weekly loader |

---

## ✅ Deliverables (per milestone)

* ✅ **Working demo**
* ✅ **Changelog + summary**
* ✅ **Known issues + roadmap for next sprint**
* ✅ Optional: system health dashboards, performance metrics, cost reports

---

## 💬 Example Supported Queries

* “What were the top 5 most liked posts this week?”
* “What did @user123 post about climate?”
* “Summarize the discourse around immigration this week.”
* “Who followed the most people this week?”

## ❌ Out-of-Scope for MVP

* Real-time streaming or ingest
* Dynamic user auth
* Full-query DSL parser
* Multi-week query windows
* Custom front-end

---

## 🔁 Fallback & Failure Modes

* LLM failure → retry → fallback model → structured output fallback
* Timeout → graceful degradation
* Invalid query → structured error message (e.g., unsupported intent)

---

## 🧪 Evaluation Metrics

* Latency per module
* Token usage per query
* Output faithfulness score (LLM eval)
* Query success rate / error rate
* Cache hit rate
* RAG quality (recall\@k, embedding similarity)

---

## 🧰 Development Stack

* Python + Streamlit for UI
* DuckDB for parquet querying
* OpenAI or Mistral for LLMs
* FAISS/Qdrant for vector store (phase 2+)
* Ray/Prefect/Temporal for orchestrator (optional for v1)
* JSON/YAML prompt templates

---

## 📈 Stakeholder Updates

Stakeholders will receive biweekly updates with:

* 🎯 Working demo walkthrough
* 🧾 Feature changelog and roadmap
* ⚠️ Known issues + plan to resolve
* 📊 Optional dashboards for token cost and system health

---

## 📍 Summary

This semantic search engine is a prototype that demonstrates a powerful, scalable architecture for working with social media data. While initially scoped to 1 week of Bluesky data, it will provide:

* A working LLM-based search engine
* Modular design for experimentation
* Production-quality engineering patterns

Future iterations can include:

* Multimodal input
* Authenticated pipelines
* Data filtering over multiple partitions
* Scalable deployment across cloud environments
