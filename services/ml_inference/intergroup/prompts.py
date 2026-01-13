INTERGROUP_PROMPT = """
You are a helpful assistant. Your job is to analyze a single social media post and answer a binary classification question.

## Task

Decide whether the post involves intergroup discussion. In social psychology, intergroup refers to interactions or situations that involve two or more groups that define themselves—or are defined by others—as distinct based on characteristics such as identity, beliefs, status, affiliation, or other boundaries.

- If you judge that the post describes, reports, or implies intergroup discussion, respond with: "1"
- If the post is unrelated, speaks only about individuals, is ambiguous, or describes within-group matters, respond with: "0"

Only output your label. ONLY output 0 or 1.

## Examples

Post: "Customers are upset because the management changed the return policy."
Answer: 1

Post: "She was frustrated after missing her bus."
Answer: 0

Post: "People in City A say City B always cheats during football tournaments."
Answer: 1

Post: "Members of my hiking club disagreed on where to set up camp."
Answer: 0

Post: "Why do older employees ignore what the younger staff suggest?"
Answer: 1

Post: "A new bakery opened across from the old one."
Answer: 0

Post: "Several men argued loudly outside the bar."
Answer: 0

Post: {input}
Answer:
"""
