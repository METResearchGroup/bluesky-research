# LLM-Specific Reflection & Debugging Rules

These rules define how the agent should reflect, debug, and recover from issues specific to its nature as a Large Language Model-based system. They are designed to enforce reliability, maintainability, and alignment with staff+ engineering practices.

## 1. Error Detection & Categorization

### When encountering an error, the agent must:

- Evaluate the failure mode using the following taxonomy:
  - **Planning Error**: Incorrect sequence or logic in breaking down the task
  - **Specification Misunderstanding**: Misinterpreted user instructions or ambiguous intent
  - **Code Generation Error**: Syntax or semantic errors in generated code
  - **Tooling Failure**: Errors due to tool misuse, file I/O, environment setup
  - **Hallucination**: Made-up APIs, nonexistent files, or invalid assumptions
  - **Out-of-Bounds Querying**: Accessing data or making references beyond known context

- Log the error type, location (e.g. line number or step), and a human-readable reason for failure to a structured `reflection_log.json`.

## 2. Self-Reflection Loop

### Whenever a task is completed or a test fails, the agent must:

- Perform a structured reflection with the following fields:
  - `what_was_attempted`: A concise description of what the agent just tried to do
  - `why_it_failed`: Explanation using the above taxonomy
  - `how_to_fix_it`: A concrete plan for resolving the issue
  - `confidence_level`: Score from 0.0 to 1.0 of how likely the fix will succeed
  - `next_steps`: The agent's recovery plan

- Append this to `reflections.md` or a structured `reflection_log.json` with timestamps.

## 3. Hallucination Control

### Before referencing tools, APIs, or file paths, the agent must:

- Check if the reference exists in the current context (file system, API manifest, etc.)
- If unsure, issue a verification query (e.g. "Does `utils.get_feature_vector` exist in any known file?")
- If the reference is speculative, clearly mark it as such (e.g. "Assuming existence of a `normalize_input()` function")

### When hallucinations are detected:

- Replace fabricated code with a placeholder and emit a comment like:
  ```python
  # TODO: This function was inferred but may not exist. Confirm before implementing.
  ```

## 4. Retry Logic

### If any step fails (e.g. failed test, syntax error, invalid plan):

- Retry the step only after:
  - Modifying inputs (e.g. refining plan, improving function signature)
  - Logging the retry rationale in the reflection log
  - Limiting to 2 retries per failure class, escalating if needed

### Escalation Triggers:

- After N retries (default N=2), stop and prompt user:
  > "Multiple retries failed for this task. Would you like me to escalate, revise the strategy, or hand off?"

## 5. Evaluation Checkpoints

At each major milestone (e.g. planning complete, tests pass, implementation written), the agent must:

- Log a checkpoint in `checkpoints.md` with:
  - `summary_of_state`: What has been accomplished
  - `open_questions`: Remaining uncertainties
  - `assumptions`: Any working assumptions that haven’t been verified

## 6. Reflection-Aware Planning

The agent must incorporate prior reflections into future planning steps. If a reflection log exists from prior runs:

- Summarize prior errors before starting new work
- Avoid repeating known mistakes
- Reuse successful strategies, as logged in past resolutions

## 7. Structured Prompt Hygiene

When constructing prompts to other models or tools, the agent must:

- Avoid nested prompts beyond 2 levels deep
- Validate all referenced variables, functions, and instructions exist in the prompt context
- Include a “reflection hook” in generated prompts, such as:
  > "Before completing this, reflect on whether the inputs are sufficient. If not, return a clarifying question."

## 8. Fail-Soft Design

- If an error is likely but non-critical (e.g. optional module missing), proceed with a warning and degraded behavior
- Log this in both reflection and summary output with clear notes to user (e.g. “Optional analysis skipped due to missing dependency”)

## 9. Controlled Creativity

- Agent is allowed to make creative design decisions **only when**:
  - No clear instruction or context exists
  - A reflective explanation is included for why a choice was made
  - The user is informed of any divergence from known standards or user patterns

## 10. Final Output Quality Check

Before returning final output to the user:

- Review generated output and run an internal checklist:
  - Does this match the original intent?
  - Are all assumptions stated?
  - Are known caveats noted?
  - Is the reflection log updated?
  - Would this pass code review by a senior engineer?

If any answer is “No”, replan or re-execute the necessary components and document why.
