# Semantic Search Engine for Bluesky Data

## High-Level Overview

The Semantic Search Engine is a sophisticated service designed to enable natural language querying over structured Bluesky social media data, typically stored in `.parquet` files. It leverages Large Language Models (LLMs) to provide semantic understanding, allowing users to ask complex questions and receive insightful answers. The system is architected for modularity, scalability, and adherence to production-grade engineering standards, aiming to transform raw social data into actionable intelligence.

Key capabilities include:
- Processing freeform natural language queries.
- Supporting diverse query types such as summarization, trend analysis, ranking, and user-specific data retrieval.
- Composing human-readable answers using a semantic layer powered by LLMs.
- Incorporating robust mechanisms for orchestration, observability, error handling (including fallbacks), and cost management.

While initial development may focus on an MVP with a Streamlit interface for demonstration, the long-term vision is a fully-fledged, scalable service designed for production environments.

## Detailed Explanation of the Code (System Architecture)

The search engine is designed as a multi-layered system, ensuring separation of concerns and enabling independent scalability of its components. The typical flow of a query through the system is as follows:

1.  **User Interface (UI Layer)**:
    *   Initially implemented with Streamlit for rapid prototyping and demonstration; in a production setting, this could be a dedicated frontend application or API endpoints.
    *   Accepts user queries via a text input or API calls.
    *   Renders formatted results, including summaries, tables, and potentially visualizations.
    *   Displays informative error messages and guidance to the user.

2.  **Gateway**:
    *   Acts as the primary entry point for all incoming queries to the backend system.
    *   Performs essential tasks such as input sanitization and validation to protect against common vulnerabilities (e.g., injection attacks).
    *   Implements rate limiting and throttling to ensure fair usage and maintain system stability under load.
    *   Can tag requests with metadata for routing, A/B testing, and experiment tracking.

3.  **Query Classifier**:
    *   Analyzes the user's natural language query to understand its intent.
    *   Classifies the query into predefined categories (e.g., "summarize discourse," "find top K posts," "lookup user activity," "trend analysis").
    *   This classification dictates the subsequent processing pipeline and resources required.

4.  **Orchestrator (Execution Directed Acyclic Graph - DAG)**:
    *   The core component responsible for managing the complex workflow of tasks needed to answer a query.
    *   Dynamically constructs and executes an execution DAG based on the classified query type.
    *   Coordinates various specialized sub-modules:
        *   **Data Load**: Efficiently loads relevant data from storage systems (e.g., `.parquet` files via DuckDB, data lakes, or databases).
        *   **Transform**: Preprocesses and transforms the loaded data. This may include text normalization, feature engineering, filtering, and aggregation to prepare data for semantic analysis.
        *   **Vector/RAG (Retrieval Augmented Generation)**: For queries requiring deep semantic understanding or searching over large text corpora. This module embeds data and queries into vector spaces, retrieves relevant documents/passages using vector databases (e.g., FAISS, Qdrant), and prepares augmented context for the LLM.
        *   **Semantic LLM Interaction**: Interfaces with configured LLMs (e.g., OpenAI, Mistral models). It utilizes versioned prompts from a Prompt Registry, sends processed data and augmented context to the LLM, and receives generated insights, summaries, or direct answers.

5.  **Answer Composer & Postprocessing**:
    *   Receives raw outputs from the LLM and other processing modules (e.g., structured data from DuckDB).
    *   Validates, cleans, merges, and formats this information into a coherent, user-friendly response.
    *   May involve applying business rules, ensuring factual consistency (where possible), and structuring the output for optimal presentation.

6.  **Presentation**:
    *   Delivers the final, composed answer back to the UI layer or API client for display to the user.

### Supporting Services

To ensure robustness, maintainability, scalability, and operational excellence, the search engine is designed to integrate with or include several critical supporting services:

*   **Prompt Registry**: A centralized, version-controlled repository (e.g., YAML/JSON files or a dedicated service) for managing prompt templates, configurations, and associated metadata for LLM interactions.
*   **LLM Fallback Layer**: Implements resilient strategies for handling LLM API failures or unsatisfactory responses. This includes retries with exponential backoff, switching to alternative (potentially smaller or cheaper) backup models, and enforcing response structure contracts.
*   **Evaluator/Scorer**: Provides tools and frameworks for systematic evaluation of system performance. This includes automated metrics (e.g., RAG recall, LLM response quality via LLM-as-judge) and infrastructure for human-in-the-loop review.
*   **Experimentation Framework**: Facilitates A/B testing and experimentation with different prompts, LLM models, RAG strategies, routing logic, and other system parameters to continuously improve performance and cost-effectiveness.
*   **Cost Monitor**: Tracks API usage (especially LLM token consumption), resource utilization, and overall operational costs. Includes mechanisms for budget alerting, enforcement, and cost attribution.
*   **Rate Limiter & Throttler**: Protects the system from abuse and overload by enforcing limits on request rates based on various criteria (e.g., user ID, IP address, API key).
*   **Data Catalog / Schema Registry**: Maintains metadata about the underlying data sources, including schemas, field descriptions, data types, provenance, and usage statistics. Essential for schema-aware querying and data governance.
*   **Model Registry**: Tracks versions and metadata of all machine learning models used within the system, including embedding models, classifier models, LLMs, and associated configurations.
*   **Observability & Logging**: Implements comprehensive structured logging, distributed tracing, and metrics collection (e.g., latency per component, error rates, token usage, cache hit rates). Feeds into monitoring dashboards and alerting systems.
*   **Caching Layer**: Implements multi-level caching strategies (e.g., caching DuckDB query results, LLM responses, embedding computations) to improve latency and reduce redundant computations and API calls.

### Development Stack Highlights
*   **Primary Language**: Python (>=3.10)
*   **UI (for MVP/Demo)**: Streamlit
*   **Data Querying & Local Processing**: DuckDB (for `.parquet` files and SQL-based operations)
*   **Large Language Models (LLMs)**: Interfaces with models from providers like OpenAI, Anthropic, Mistral AI, or open-source models.
*   **Vector Storage & RAG**: Technologies like FAISS, Qdrant, or managed vector database services.
*   **Orchestration Framework (Potential)**: Tools like Ray, Prefect, or Dagster for managing complex data and ML pipelines.
*   **Prompt Management**: Typically JSON or YAML templates, potentially managed via a dedicated registry.

## Testing Details

A rigorous testing strategy is fundamental to ensuring the reliability, correctness, and performance of the Semantic Search Engine. All tests will be located within the `search_engine/tests/` directory and will adhere to the project's established high standards for code quality and test coverage.

The testing approach will include:

*   **Unit Tests**:
    *   These tests will focus on verifying the correctness of individual functions, methods, and classes in isolation. External dependencies (such as LLM APIs, databases, or other microservices) will be thoroughly mocked.
    *   Example test files and their purpose:
        *   `search_engine/tests/unit/test_query_classifier.py`: Validates the logic for accurately categorizing various natural language queries into their respective intents.
        *   `search_engine/tests/unit/test_data_loader.py`: Ensures correct loading, parsing, and initial validation of data from `.parquet` files or other sources.
        *   `search_engine/tests/unit/test_prompt_manager.py`: Tests the functionality of the prompt registry, including template rendering and version management.
        *   `search_engine/tests/unit/test_answer_composer.py`: Verifies that the answer composer correctly formats and structures outputs from different data sources and LLM responses.
        *   `search_engine/tests/unit/test_llm_fallback.py`: Checks the behavior of the LLM fallback layer under simulated API error conditions or policy violations.

*   **Integration Tests**:
    *   These tests will verify the interactions and data flow between different components or modules of the system. They ensure that integrated parts work together as expected.
    *   Example integration scenarios:
        *   Testing the pipeline from the Gateway, through the Query Classifier, to the Orchestrator (with key sub-modules like Data Load and LLM Interaction mocked or using test doubles).
        *   Verifying the complete RAG pipeline: text chunking, embedding generation, vector store retrieval, and context assembly for the LLM.
        *   Ensuring that the Caching Layer correctly stores and retrieves LLM responses or intermediate computation results.

*   **End-to-End (E2E) Tests**:
    *   These tests will simulate real user queries and validate the entire system flow, from the query input (e.g., via an API endpoint) to the final response presented to the user.
    *   E2E tests will typically run against a controlled, representative dataset. For critical paths involving LLMs, these tests might make limited calls to actual (sandboxed or development-tier) LLM APIs to ensure real-world compatibility, subject to cost and flakiness considerations.
    *   Example E2E test case:
        *   Input: A query like "What were the main topics discussed by @username regarding 'environmental policy' last month?"
        *   Validation: Check if the system returns a coherent summary, relevant post snippets, and correct metadata, and that the response structure matches the API contract.

*   **Property-Based Testing**:
    *   For components with complex logic or a wide range of possible inputs (e.g., data transformation functions, input sanitizers), property-based testing (using libraries like Hypothesis) will be employed to generate diverse test cases automatically and verify invariants.

All tests will be designed to be independent, idempotent, and automated as part of the CI/CD pipeline. The goal is to maintain high test coverage (e.g., >90% line coverage, >80% branch coverage) and use tests to drive development and prevent regressions.
