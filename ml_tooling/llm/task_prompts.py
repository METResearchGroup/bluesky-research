"""Prompts for specific tasks."""
political_ideology_prompt = """
Imagine that you are a political ideology classifier, specifically focused on the context of United States politics. \
Please classify the text denoted in <text> based on the political lean of the opinion or argument \
it presents. Your options are 'left-leaning', 'moderate', 'right-leaning', or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. Only provide the label \
("left-leaning", "moderate", "right-leaning", "unclear") in your response. Justifications are not necessary. \
""" # noqa

civic_prompt = """
Pretend that you are a classifier that predicts whether a post has civic content or not. Civic refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as “civic”; otherwise they are not civic. \
Please classify the following text denoted in <text> as "civic" or "not civic". Only provide "civic" or \
"not civic" in your response. Justifications are not necessary. \
"""


task_name_to_task_prompt_map = {
    "political_ideology": political_ideology_prompt,
    "civic": civic_prompt
}
