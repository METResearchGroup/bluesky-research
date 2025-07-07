# Task Planning and Prioritization

This document outlines the rules for planning and prioritizing tasks to ensure the coding agent delivers high-value features efficiently and autonomously. These guidelines enable the agent to break down complex tasks, estimate effort accurately, align work with user and stakeholder goals, and scale to enterprise-level projects, reflecting the standards of a staff or principal engineer.

## Task Planning

Task planning involves decomposing user requests into actionable subtasks, estimating effort, identifying dependencies, and validating feasibility to ensure systematic progress.

- Decompose user requests into granular subtasks, each with a single responsibility, to facilitate focused implementation and testing. For example, a request to build a login feature should be broken into subtasks like designing the UI, implementing authentication logic, and writing tests.
- Define clear deliverables for each subtask, specifying expected inputs, outputs, and success criteria. For instance, the authentication logic subtask should deliver a function that validates credentials against a database, returning a session token on success.
- Identify dependencies between subtasks and across features or projects to determine the execution order. For example, the UI subtask depends on the authentication logic, and a feature may depend on a DevOps pipeline setup.
- Estimate effort for each subtask in hours, using historical data or industry benchmarks (e.g., API endpoint: 2-4 hours, database migration: 4-8 hours, complex UI component: 8-12 hours). Document assumptions, such as familiarity with the tech stack, in a `plan_<feature>.md` file.
- Create a task plan in a `plan_<feature>.md` file, summarizing subtasks, deliverables, dependencies, and effort estimates in a Markdown table (e.g., `| Subtask | Deliverable | Dependencies | Effort (hrs) |`). Commit to version control with a message like `[plan] Define task plan for <feature>`.
- Validate the task plan against user requirements by cross-referencing deliverables with the original request. If discrepancies arise, infer intent from historical tasks in `lessons_learned.md` and propose a minimal viable solution, seeking user confirmation within 24 hours.
- Simulate the task plan before execution by mapping dependencies and effort to a Gantt chart, identifying infeasible timelines or missing steps. Write a Python script to check `plan_<feature>.md` for missing dependencies or effort estimates, failing if any subtask lacks a deliverable.
- Reassess the task plan daily or if new requirements increase effort by >20% or introduce new dependencies, updating `plan_<feature>.md` and committing changes with explanations.

## Multi-Feature and Roadmap Planning

To scale planning across projects and align with long-term goals, the agent maintains a roadmap and coordinates cross-feature tasks.

- Create a `roadmap.md` file for features spanning multiple sprints, outlining high-level goals, milestones, and cross-feature dependencies. Map each feature to an OKR (Objective and Key Result) to ensure alignment with measurable outcomes.
- Validate roadmap priorities with stakeholders (e.g., product managers, developers) via a summary email, incorporating feedback within 24 hours. Document stakeholder input in `roadmap.md`.
- Limit active subtasks to 5-7 per day, based on estimated effort, to avoid overloading processing capacity. If capacity is exceeded, defer low-priority tasks and update `plan_<feature>.md`.
- Maintain a `dependencies.md` file listing all cross-project dependencies (e.g., shared libraries, external team deliverables), updated weekly to reflect resolved or new blockers.

## Prioritization

Prioritization ensures the agent focuses on high-impact tasks, balancing user needs, technical constraints, and project goals.

- Prioritize subtasks based on user impact, defined as the value delivered to the user (e.g., core functionality over polish). For example, prioritize login authentication over password strength indicators.
- Consider technical dependencies, ensuring foundational subtasks (e.g., database schema setup) are completed before dependent ones (e.g., API endpoints).
- Account for risk and complexity, prioritizing high-risk or complex subtasks early to surface issues sooner. For instance, tackle a novel algorithm before UI refinements.
- Use a scoring system to rank subtasks, assigning 1-5 points for user impact, risk, and urgency, then summing the scores. Execute subtasks with higher scores first, breaking ties by favoring lower effort unless a critical dependency exists.
- Document prioritization decisions in `plan_<feature>.md`, including the scoring rationale and trade-offs (e.g., delaying a feature to unblock another). Use a table format: `| Subtask | Impact | Risk | Urgency | Total Score |`.
- Re-evaluate priorities after completing each subtask or receiving new user input, updating `plan_<feature>.md` if rankings change. If a high-priority task emerges, pause low-priority subtasks and notify the user.
- Resolve prioritization conflicts using a decision tree: (1) Compare total scores; (2) If equal, prioritize lower effort; (3) If still tied, favor tasks unblocking critical dependencies. For unresolved conflicts, escalate to the user with a table listing subtasks, scores, pros/cons, and a recommended choice. If no response within 48 hours, proceed with the recommended option, logging the decision.
- Validate prioritization outcomes post-feature by measuring user impact (e.g., feature usage metrics) and comparing to scores. Log discrepancies in `lessons_learned.md`.

## Dependency Management

Managing dependencies ensures smooth task execution by addressing blockers and external requirements.

- Map all dependencies in `plan_<feature>.md` and `dependencies.md`, including internal (e.g., other subtasks) and external (e.g., third-party APIs, DevOps deliverables). For example, note that an API subtask requires a service key.
- Mitigate external dependency risks by identifying alternatives or fallbacks. For instance, if a third-party API is unavailable, mock it with a stub returning sample data, documented in `mocks.md`.
- Research alternative solutions for missing dependencies (e.g., open-source libraries) and propose them in `plan_<feature>.md` before escalating. For example, replace a delayed API with a community-maintained equivalent.
- Communicate dependency blockers to the user promptly, proposing workarounds or adjusted timelines (e.g., parallelizing UI development during a database migration delay). Use a template: `Blocker: <issue>. Impact: <delay>. Proposed Solution: <workaround>.`
- Update `plan_<feature>.md` and `dependencies.md` to reflect resolved dependencies, ensuring the execution order remains valid. Commit changes with messages like `[plan] Update dependencies for <feature>`.
- Assign ownership for shared dependencies using a RACI matrix in `dependencies.md`, clarifying who is Responsible, Accountable, Consulted, and Informed.

## Progress Tracking and Self-Reflection

Tracking progress and reflecting on outcomes ensure alignment with goals and continuous improvement.

- Update `plan_<feature>.md` after completing each subtask, marking it as done and noting deviations from estimated effort or deliverables. For example, if a subtask took 6 hours instead of 4, document the reason (e.g., edge cases).
- Maintain a `todo.md` file as a checklist of subtasks, synchronized with `plan_<feature>.md`, following `AGENT_WORKFLOW.md`. Update markers (e.g., `[x]`) immediately after completion.
- Conduct self-reflection after major milestones (e.g., feature completion), comparing actual outcomes to planned deliverables. Document findings in `lessons_learned.md`, covering successes, challenges, and process improvements (e.g., “Underestimated UI complexity due to responsive design”).
- Compare actual vs. estimated effort for each subtask in `lessons_learned.md`, updating benchmarks in `plan_<feature>.md` if deviations exceed 20%. For example, increase database task estimates by 10% if consistently underestimated.
- Verify task completion by running all relevant tests (per `tdd.md`) and validating against user requirements. If gaps exist, create new subtasks and update `plan_<feature>.md`.
- Instrument task completion times with Prometheus metrics to identify bottlenecks (e.g., subtasks taking >150% of estimated effort). Log metrics in a `metrics.md` file.

## Error Handling and Recovery

Errors during planning or prioritization require swift detection and correction to maintain progress.

- Detect planning errors (e.g., missed dependencies) by reviewing `plan_<feature>.md` before execution and after updates. Correct errors by revising the plan and notifying the user if delays exceed 1 day.
- Handle prioritization errors (e.g., focusing on low-impact tasks) by re-scoring subtasks during daily reassessments. Adjust the execution order and document the rationale in `plan_<feature>.md`.
- Implement retry logic for transient issues (e.g., temporary API unavailability) with up to three attempts using exponential backoff (1s, 2s, 4s). Log attempts in `logs.md`.
- Escalate persistent errors to the user with a proposed solution and timeline, using a template: `Error: <issue>. Impact: <effect>. Solution: <fix>. ETA: <time>.` If no response within 48 hours, proceed with the proposed fix, logging the decision.
- For ambiguous requirements, infer intent from `lessons_learned.md` and propose a minimal viable solution, seeking confirmation. If unresolvable, prototype both options and A/B test with a user subset, selecting the higher-performing option.

## Integration with Other Processes

Task planning integrates with coding, testing, and workflow rules for cohesive feature delivery.

- Align task plans with the TDD workflow in `tdd.md`, ensuring each subtask includes test cases as deliverables. For example, an API endpoint subtask must include unit and integration tests.
- Follow `CODING_RULES.md` for effort estimation, accounting for complexity limits (e.g., cyclomatic complexity <10) and testing requirements (e.g., >90% coverage).
- Synchronize progress with `AGENT_WORKFLOW.md`, using `todo.md` for detailed tracking and `plan_<feature>.md` for high-level planning. Ensure updates reflect in both.
- Incorporate `UI_RULES.md` for UI subtasks, prioritizing clarity and accessibility in deliverables.
- Communicate progress and escalations per `AGENT_CONVERSATION_STYLE.md`, using prose for updates and natural language lists (e.g., “current blockers include: x, y, z”) for summaries.
- Sync `plan_<feature>.md` with a Jira board, creating tickets for each subtask and updating statuses automatically. Expose plan updates via a REST API for external tool queries.

## Risk Management

Proactive risk management mitigates potential blockers and ensures robust planning.

- Create a risk register in `plan_<feature>.md`, listing potential blockers (e.g., API downtime, skill gaps) with likelihood (1-5), impact (1-5), and mitigation plans. For example: `Risk: API downtime. Likelihood: 3. Impact: 4. Mitigation: Mock API and monitor status.`
- Review the risk register daily, updating mitigations and escalating high-impact risks (score >12) to the user with proposed actions.
- Maintain a Kanban board in `plan_<feature>.md` with columns for To Do, In Progress, and Done, updating daily to visualize bottlenecks and prioritize risk mitigations.

## Archival and Scalability

To support enterprise-scale projects, task plans are organized, archived, and indexed for maintainability.

- Organize task plans in a `plans/` directory, with one `plan_<feature>.md` per feature, linked in `roadmap.md`. For projects with >50 subtasks, store plans in a SQLite database, syncing with `plan_<feature>.md` for readability.
- Archive completed plans in `plans/archive/`, tagged with feature name and completion date (e.g., `plan_login_2025-07-07.md`). Index `lessons_learned.md` entries with keywords (e.g., ‘database’, ‘UI’) for retrieval.
- Version task plans in git, committing changes with messages like `[plan] Update <feature> plan for <change>`. Backup `plans/` to a cloud storage service weekly.

## Implementation Notes

- Use Markdown tables in `plan_<feature>.md` for subtasks, dependencies, and priorities. Example: `| Subtask | Deliverable | Dependencies | Effort (hrs) | Priority Score |`.
- Test task plans for completeness with a Python script checking for missing fields, integrated into CI pipelines (per `CODING_RULES.md`).
- Optimize planning for performance by limiting subtasks to 10-20 per feature, merging or splitting to balance granularity and manageability.
- Ensure compatibility with Streamlit or other frameworks by including framework-specific considerations in effort estimates (e.g., Streamlit’s rendering constraints).
- Instrument planning efficiency with Prometheus metrics (e.g., plan reassessment frequency) and set alerts for error rates >1% over a 5-minute window.