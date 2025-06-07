# Progress Update Markdown Format

This document describes the required format and content for all progress update markdown files in this folder. Follow these guidelines to ensure consistency, clarity, and traceability across all updates.

---

## File Naming
- Use the format: `<number>_<short_description>.md`
  - Example: `16_ui_discover_tab_updates.md`
- Number sequentially based on update chronology.

## Structure & Required Sections

### 1. Title
- Format: `# <number>. <Short Description> (<YYYY-MM-DD>)`
- Example: `# 16. UI Discover New Leads Tab Completion (2025-05-26)`

### 2. Summary
- Brief, high-level overview of what was accomplished in this update.
- Use bullet points for clarity.

### 3. Implementation Details
- List key technical changes, refactors, or new features.
- Use bullet points for each major item.
- Include file/module names where relevant.

### 4. Testing
- Describe how the changes were tested (manual, integration, unit tests).
- List specific tests run and their outcomes.
- Note any planned or pending tests.

### 5. Remaining Work
- Bullet list of any follow-up tasks, improvements, or known issues.
- Use bold for section headers within this list if needed.

### 6. References
- List related planning docs, previous updates, or relevant files.
- Use relative paths where possible.

---

## Style & Formatting Guidelines
- Use Markdown headers (`#`, `##`, etc.) for section separation.
- Use bullet points for lists; avoid long paragraphs.
- Use bold for important terms or section headers within lists.
- Keep language concise and actionable.
- Include dates in the title for traceability.
- If a section is not applicable, include it with a note (e.g., `## Remaining Work\n- None at this time.`)
- For multi-phase updates, clearly indicate which phase is covered.

---

## Example

```
# 17. Feature XYZ Refactor (2025-06-01)

## Summary
- Major refactor of the XYZ feature for performance and maintainability.

## Implementation Details
- Rewrote `xyz.py` to use async I/O.
- Updated all dependent modules and tests.

## Testing
- Ran all unit and integration tests: 100% pass rate.
- Manual validation of edge cases.

## Remaining Work
- **Docs:** Update user and developer documentation.
- **Performance:** Profile under production load.

## References
- See also: `16_ui_discover_tab_updates.md`, `xyz_refactor_plan.md` 
