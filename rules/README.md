# Rules Directory Overview

This directory contains the canonical rules, style guides, and process documentation for all engineering, product, and agentic workflows in the Bluesky-Research project. Use this README to quickly locate the right guide for any task, and to understand how the rules interconnect.

---

## 📦 File Summaries & Usage

### HOW_TO_START_NEW_PROJECT.md
**Purpose:** Step-by-step guide for starting a new Linear project, from spec creation to ticket generation and file organization. 
**References:** HOW_TO_WRITE_A_SPEC.md, HOW_TO_WRITE_LINEAR_PROJECT.md, HOW_TO_WRITE_LINEAR_TICKET.md. 
**When to use:** Start here when beginning any new project.

### HOW_TO_WRITE_A_SPEC.md
**Purpose:** Walks you through creating a well-scoped, cross-functional spec ready for Linear. Includes stakeholder-aligned phases and a structured output format. 
**References:** Mentions that after spec approval, you should proceed to HOW_TO_WRITE_LINEAR_PROJECT.md and HOW_TO_WRITE_LINEAR_TICKET.md. 
**When to use:** Use when drafting a new project or feature specification.

### HOW_TO_WRITE_LINEAR_PROJECT.md
**Purpose:** Defines the canonical structure and tone for authoring Linear projects, including problem statement, objectives, scope, timeline, stakeholders, risks, and related work. 
**References:** Suggests referencing planning docs and related tickets. 
**When to use:** Use when creating a new Linear project from an approved spec.

### HOW_TO_WRITE_LINEAR_TICKET.md
**Purpose:** The canonical style guide for writing Linear tickets. Specifies required sections (title, context, requirements, test plan, etc.), best practices, and an example template. 
**References:** Referenced by HOW_TO_START_NEW_PROJECT.md and HOW_TO_WRITE_A_SPEC.md. 
**When to use:** Use for every ticket you write or review.

### WHAT_MAKES_A_GREAT_SPEC.md
**Purpose:** Outlines evaluation criteria for specs from the perspective of PMs, engineers, designers, analytics, and cross-functional stakeholders. 
**References:** Complements HOW_TO_WRITE_A_SPEC.md. 
**When to use:** Use to review or improve a spec before project kickoff.

### WHAT_MAKES_A_GREAT_PROJECT.md
**Purpose:** Lists criteria for great Linear projects from the perspective of engineers, PMs, and staff engineers. 
**References:** Complements HOW_TO_WRITE_LINEAR_PROJECT.md. 
**When to use:** Use to review or improve a Linear project before launch.

### WHAT_MAKES_A_GREAT_TICKET.md
**Purpose:** Details what makes a great project management ticket, with criteria for engineers, PMs, and staff engineers. 
**References:** Complements HOW_TO_WRITE_LINEAR_TICKET.md. 
**When to use:** Use to review or improve a ticket before assignment or implementation.

### TASK_PLANNING_AND_PRIORITIZATION.md
**Purpose:** Comprehensive rules for decomposing, prioritizing, and tracking tasks, including dependency management, progress tracking, and Linear/GitHub integration. 
**References:** References CODING_RULES.md, AGENT_WORKFLOW.md, LLM_REFLECTION_DEBUGGING_RULES.md, UI_RULES.md, GITHUB_OPERATIONS.markdown. 
**When to use:** Use for planning, breaking down, or prioritizing any project or feature.

### AGENT_CONVERSATION_STYLE.md
**Purpose:** Defines the agent's communication style, tone, formatting, and self-monitoring rules for all user interactions. 
**References:** Referenced by LLM_REFLECTION_DEBUGGING_RULES.md and GITHUB_OPERATIONS.markdown. 
**When to use:** Use when writing documentation, PRs, or communicating with users.

### LLM_REFLECTION_DEBUGGING_RULES.md
**Purpose:** Rules for agentic self-reflection, error logging, debugging, and recovery, with integration into planning and GitHub workflows. 
**References:** Integrates with TASK_PLANNING_AND_PRIORITIZATION.md and GITHUB_OPERATIONS.markdown. 
**When to use:** Use when debugging, reflecting on errors, or writing reflection logs.

### GITHUB_OPERATIONS.markdown
**Purpose:** Canonical guide for all GitHub operations (branching, PRs, commits, error handling) using the GitHub CLI and Linear integration. 
**References:** References CODING_RULES.md, AGENT_CONVERSATION_STYLE.md, TASK_PLANNING_AND_PRIORITIZATION.md, LLM_REFLECTION_DEBUGGING_RULES.md. 
**When to use:** Use for any GitHub-related workflow, especially when linking PRs to Linear tickets.

### UI_RULES.md
**Purpose:** UI design principles and implementation notes for clarity, accessibility, and business value communication. 
**References:** Referenced by TASK_PLANNING_AND_PRIORITIZATION.md for UI subtasks. 
**When to use:** Use when designing or reviewing UI/UX for any feature.

### AGENT_WORKFLOW.md
**Purpose:** Outlines the agentic workflow, including todo.md management, file rules, and references to other process docs. 
**References:** Referenced by TASK_PLANNING_AND_PRIORITIZATION.md. 
**When to use:** Use for managing task progress and file operations.

### CODING_RULES.md
**Purpose:** Defines code quality, architecture, testing, style, performance, and debugging standards for all development. 
**References:** Referenced by TASK_PLANNING_AND_PRIORITIZATION.md, GITHUB_OPERATIONS.markdown, and LLM_REFLECTION_DEBUGGING_RULES.md. 
**When to use:** Use for all coding, testing, and code review activities.

### HOW_TO_WRITE_RULES.md
**Purpose:** Guidelines for writing new rules documents, including required sections, formatting, and review process. 
**References:** None directly, but sets standards for all rules files. 
**When to use:** Use when authoring or updating any rules or process documentation.

### RESOURCES.md
**Purpose:** Curated list of external resources for agentic workflows, prompt engineering, and best practices. 
**References:** None. 
**When to use:** Use for further reading or to improve prompt and workflow quality.

---

## 🧭 Quick Start Recommendations
- **Starting a new project?** Begin with `HOW_TO_START_NEW_PROJECT.md`.
- **Writing a spec?** Use `HOW_TO_WRITE_A_SPEC.md` and review with `WHAT_MAKES_A_GREAT_SPEC.md`.
- **Creating a Linear project?** See `HOW_TO_WRITE_LINEAR_PROJECT.md` and `WHAT_MAKES_A_GREAT_PROJECT.md`.
- **Writing tickets?** Use `HOW_TO_WRITE_LINEAR_TICKET.md` and `WHAT_MAKES_A_GREAT_TICKET.md`.
- **Planning or prioritizing work?** Reference `TASK_PLANNING_AND_PRIORITIZATION.md`.
- **Coding or reviewing code?** Follow `CODING_RULES.md`.
- **Managing GitHub/PRs?** Use `GITHUB_OPERATIONS.markdown`.
- **Debugging or reflecting?** See `LLM_REFLECTION_DEBUGGING_RULES.md`.
- **Designing UI?** Reference `UI_RULES.md`.
- **Writing or updating rules?** Use `HOW_TO_WRITE_RULES.md`.

---

For any process, always check for cross-references in the relevant file descriptions above to ensure you are following all required standards and dependencies.
