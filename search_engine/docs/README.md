# 📚 Project Documentation

Welcome to the documentation and thought leadership hub for the **Bluesky Semantic Search Engine**. This folder contains high-level design documents, technical specifications, evaluations, and project thinking. All runtime artifacts (e.g. code, prompts, pipelines) are tracked outside of this folder.

---

## 🧭 What You'll Find Here

### 🔧 Architecture
- System overview and high-level diagrams
- Data flow and modular design breakdown

### 🧠 Orchestration
- Execution DAG layout and module interaction patterns
- Retry and fallback strategies

### 📏 Evaluation
- Framework for evaluating LLM responses
- Metrics, sample outputs, scoring methodology

### 🪬 Observability
- Logs, traces, dashboards, performance instrumentation

### 💰 Cost and Performance
- Token usage monitoring, latency benchmarks, cache efficiency

### 💣 Failure Modes
- Graceful degradation paths, fallback matrix, error handling design

### 📊 Data & Schema
- Field definitions, registry structure, dynamic data loading

### ✍️ Thought Leadership
- Blog-draft-style articles on engineering decisions, lessons learned, and deeper dives

### 📓 Dev Journal
- Prompt iterations, bug logs, session notes, in-progress ideas

---

## ✍️ Contributing

Each subfolder includes a `RULES.md` file that outlines:
- Best practices for writing and updating artifacts
- Target audience and purpose
- Style and formatting guidelines
- "Definition of done" criteria

---

## 🔗 Related Resources

- Prompt Registry and Production Assets: `../prompts/` (or wherever you're storing them)
- Source Code and Services: `/src`, `/pipeline`, `/infra`, etc.
- Milestone Tracker: [Notion/JIRA/Linear URL if applicable]
- Artifact Tracker: [`artifact-tracker.md`](./artifact-tracker.md)

---

## 🧼 Philosophy

This folder is optimized for clarity, learning, and reuse — not just for yourself, but for future collaborators and readers. Every document here should strive to explain **what was built, why it was built that way, and what was learned along the way**.
