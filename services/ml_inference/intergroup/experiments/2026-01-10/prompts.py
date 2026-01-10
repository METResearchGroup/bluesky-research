INTERGROUP_PROMPT = """
You are a helpful assistant. Your job is to analyze a single social media post and answer a binary classification question.

## Task

Decide whether the post involves intergroup conflict. In social psychology, intergroup refers to interactions or situations that involve two or more groups that define themselves—or are defined by others—as distinct based on characteristics such as identity, beliefs, status, affiliation, or other boundaries.

**Intergroup conflict** occurs when there is evidence of disagreement, tension, hostility, competition, or negative comparison *between* two or more such groups. The groups may be based on race, ethnicity, nationality, religion, sports teams, professions, age, political affiliation, interests, or any other socially meaningful division. Posts do *not* need to explicitly mention "groups": look for implicit references, stereotypes, or us-vs-them dynamics.

- If you judge that the post describes, reports, or implies intergroup conflict, respond with: "1"
- If the post is unrelated, speaks only about individuals, is ambiguous, or describes within-group matters, respond with: "0"

Only output your label and a brief explanation in the prescribed format.

## Examples

Post: "Customers are upset because the management changed the return policy."
Answer: 1
[Conflict between customers and management—two social groups with differing interests.]

Post: "She was frustrated after missing her bus."
Answer: 0
[No reference or implication of social groups.]

Post: "People in City A say City B always cheats during football tournaments."
Answer: 1
[Clear group-based antagonism between people from different cities.]

Post: "Members of my hiking club disagreed on where to set up camp."
Answer: 0
[This is within-group disagreement, not between distinct groups.]

Post: "Why do older employees ignore what the younger staff suggest?"
Answer: 1
[Conflict is described between age groups.]

Post: "A new bakery opened across from the old one."
Answer: 0
[No conflict or reference to group-based tension.]

Post: "Several men argued loudly outside the bar."
Answer: 0
[No evidence of social groups being referenced.]
"""
