# TODO: run this against the generated SQL query in order to add the business logic (e.g., LIMIT 10). Can also include SQL cleanup logic here as well.

Something like this:

```python
        sql = content.strip()

        # Remove markdown code blocks if present
        if sql.startswith("```"):
            lines = sql.split("\n")
            # Remove first line (```sql or ```)
            lines = lines[1:]
            # Remove last line if it's ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            sql = "\n".join(lines).strip()
```