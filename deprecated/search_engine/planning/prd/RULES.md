# PRD Writing Guidelines

## 📝 Required Sections

### 1. Objective
- Clear, concise statement of what the PRD aims to achieve
- Should be measurable and time-bound
- Include key success metrics

### 2. Scope
- Explicitly list what is in-scope (✅)
- Explicitly list what is out-of-scope (❌)
- Use clear boundaries to prevent scope creep

### 3. Task Breakdown
- Break down into logical groupings (e.g., UI, Data, API)
- Each task should have:
  - Unique ticket number (e.g., #001)
  - Clear description
  - Sub-tasks or acceptance criteria
  - Estimated time (if possible)
  - Dependencies (if any)

### 4. Acceptance Criteria
- Must be specific and measurable
- Include both functional and non-functional requirements
- Cover edge cases and error scenarios
- Define performance metrics where relevant

### 5. Example Queries/Use Cases
- Provide concrete examples of supported functionality
- Include edge cases and error scenarios
- Show expected input/output pairs

### 6. Data Models & Schemas
- Document all data structures
- Include field types and constraints
- Show relationships between models
- Document any API contracts

### 7. Timeline & Resources
- Estimated completion time
- Required team members/roles
- Dependencies on other teams/systems
- Milestone dates

### 8. Deliverables
- List of concrete outputs
- Documentation requirements
- Testing requirements
- Deployment requirements

## 🎨 Formatting Guidelines

### Headers
- Use clear, hierarchical headers
- Include emojis for visual scanning
- Keep consistent with project style

### Lists
- Use bullet points for related items
- Use numbered lists for sequential steps
- Indent sub-items appropriately

### Code & Technical Details
- Use code blocks for examples
- Include type annotations
- Document assumptions and constraints

## 🔍 Quality Checklist

- [ ] All sections are complete
- [ ] Scope is clearly defined
- [ ] Tasks are properly broken down
- [ ] Acceptance criteria are measurable
- [ ] Examples are concrete and relevant
- [ ] Dependencies are identified

## 📝 File Naming Convention

### Format
- Files should be named using the pattern: `<date>_<prd_task>.md`
- Date format: YYYY_MM_DD
- Task name should be descriptive and use underscores
- Example: `2025_06_03_semantic_search_prd.md`

### Guidelines
- Use consistent date formatting
- Keep task names concise but descriptive
- Avoid special characters except underscores
- Maintain chronological order in directory

### Examples
- `2025_06_03_semantic_search_prd.md`
- `2025_06_10_user_analytics_prd.md`
- `2025_06_17_content_recommendation_prd.md`
