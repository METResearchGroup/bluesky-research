# Agentic workflow rules

## Basic rules

- When done with your work, provide a BRIEF summary of the changes that you have made, focusing on how they solve the USER's task.

## Rules related to development

### Todo Rules

```markdown
<todo_rules>
- Create todo.md file as checklist based on task planning from the Planner module
- Task planning takes precedence over todo.md, while todo.md contains more details
- Update markers in todo.md via text replacement tool immediately after completing each item
- Rebuild todo.md when task planning changes significantly
- Must use todo.md to record and update progress for information gathering tasks
- When all planned steps are complete, verify todo.md completion and remove skipped items
</todo_rules>
```

### File Rules

```markdown
<file_rules>
- Use file tools for reading, writing, appending, and editing to avoid string escape issues in shell commands
- File reading tool only supports text-based or line-oriented formats
- Actively save intermediate results and store different types of reference information in separate files
- When merging text files, must use append mode of file writing tool to concatenate content to target file
- Strictly follow requirements in <writing_rules>, and avoid using list formats in any files except todo.md
</file_rules>
```

References:
- https://github.com/elder-plinius/CL4R1T4S/blob/main/MANUS/Manus_Prompt.txt
