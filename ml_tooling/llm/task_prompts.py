"""Prompts for specific tasks."""
political_ideology_prompt = """
Imagine that you are a political ideology classifier, specifically focused on the context of United States politics. \
Please classify the text denoted in <text> based on the political lean of the opinion or argument \
it presents. Your options are 'democrat', 'republican', or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. Only provide the label \
("democrat", "republican", "unclear") in your response. \

Return in a JSON format in the following way:
{
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason": <optional, a 1 sentence reason for why the text has the given political ideology>
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
"""  # noqa

civic_prompt = """
Pretend that you are a classifier that predicts whether a post has civic content or not. Civic refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
wealth differences, racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as “civic”; otherwise they are not civic. \
Please classify the following text as "civic" or "not civic". Only provide "civic" or \
"not civic" in your response. \

Return in a JSON format in the following way:
{
    "civic": <two values, 'civic' or 'not civic'>,
    "reason": <optional, a 1 sentence reason for why the text is civic>
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
"""  # noqa

civic_and_political_ideology_prompt = """
You are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "left", "right", or 'unclear'. \
If the text is not sociopolitical, return "unclear". Base your response on US politics.\

Think through your response step by step.

Return in a JSON format in the following way:
{
    "is_sociopolitical": <boolean, two values, True or False>,
    "political_ideology_label": <string, three values, 'left', 'right', 'unclear'. If the post is not sociopolitical, return an empty string, "">,
    "reason_sociopolitical": <string, a 1 sentence reason for why the text is sociopolitical or not.>,
    "reason_political_ideology": <Optional[str], a 1 sentence reason for why the text has the given political ideology or is unclear. If the post is not sociopolitical, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
"""  # noqa

toxicity_prompt = """
Pretend that you are a toxicity classifier. We define a post having toxicity as \
a rude, disrespectful, or unreasonable comment that is likely to make people leave a discussion. \
This content is harmful, offensive, or damaging, and is usually characterized \
by negativity, aggression, or disrespect. 

Here are some examples of toxic posts:
- Anyone who supports [Politician's Name] is a fool and lacks any understanding of real issues. They should be ashamed to show their face in public!": This post is aggressive and disrespectful.
- "The government is controlled by a secret society bent on destroying our freedom. Do not trust any official statements!": This post promotes conspiracy theories.
- "Share if you think [Politician's Name] should be jailed for their crimes against the state! Let's make this go viral to force action!": This post is aggressive and promotes violence.
- "[Opposing political group] are like vermin, they spread disease and discord wherever they go. We need to cleanse our society!": This post is dehumanizing and promotes violence.
- "Real Americans support [Policy or Politician], and the rest are enemies of the state who want to see our nation fail!" This post is divisive and promotes an "us vs. them" mentality.
- "The recent election was completely rigged! [Provides falsified data and conspiracies about voter fraud with no evidence]": This post promotes misinformation and conspiracy theories.
- "We need to fight back against these politicians with whatever it takes! They deserve to be scared of us!": This post promotes violence and aggression.
- "I can't believe how stupid people are to believe in [Policy or Politician]. They are all brainwashed and need to wake up!": This post is disrespectful and aggressive.
- "Anyone who likes that movie has the worst taste and probably no real friends.": This post is rude and disrespectful.
- "Can you believe how much weight she's gained? It's disgusting. She should stop posting pictures until she looks decent.": This post is body-shaming and disrespectful.
- "It's just a fact that some races are naturally more violent and less intelligent. Don't be fooled by political correctness.": This post is racist and promotes harmful stereotypes.
- "I heard that she cheated on all her exams. That's the only way she could have passed. Don't trust a word she says!": This post is defamatory and harmful.
- "He's such a loser, always trying too hard in sports but still failing.": This post is disrespectful and aggressive.

Please classify the following text denoted in <text> as "toxic" or "not toxic".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "toxicity": <two values, 'toxic' or 'not toxic'>,
    "reason": <optional, a 1 sentence reason for why the text is toxic>
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
"""  # noqa

constructiveness_prompt = """
Pretend that you are a constructiveness classifier. Our purpose is to promote \
content that is likely to lead to constructive conversations online. We want \
to classify posts as constructive if they have some of the following characteristics:
- Nuance: Incorporates multiple points of view in an attempt to provide a full picture or contribute useful detail and/or context. \
- Affinity: References shared interests, motivations or outlooks between the comment author and another individual, group or entity. \
- Politeness and Respect: Uses polite language and shows respect for differing opinions. \
- Fact-based: Uses information that is accurate and well-researched. \
- Use of Examples and Evidence: Incorporates relevant examples and evidence. \
- Solution-oriented: Seeks solutions or proposes constructive ideas for problems. \
- Educational: Contributions that aim to inform or clarify complex topics help in raising the quality of discussion. \
- Encouragement and Support: Offers support or positive reinforcement. \
- Promotes and encourages wellbeing, especially for the wider community \
- Avoidance of aggressive, confrontational, sarcastic, or mocking language. \

Here are some examples of constructive posts:
- "Painting has always helped me unwind. Does anyone else here use art for stress relief? What kind of art do you do?": Invites knowledge sharing and discussion.
- "Remember to take a little time for yourself this weekend. A little rest can boost our productivity and mood!": Promotes a culture of wellness in the community.
- "Let's appreciate both the challenges and benefits that digital education brings to our children.": This post acknowledges multiple aspects of a topic.
- "Diverse teams often come up with more innovative solutions because they bring different perspectives to the table.": This informs about the benefits of diversity.
- "Celebrating cultural festivals can help promote understanding and appreciation among diverse community groups.": This post encourages affinity.
- "Choosing locally grown produce supports local farmers and reduces carbon footprint.": Fact-based.
- "Celebrating women in STEM roles can inspire future generations to pursue careers in these fields.": Encouragement and Support. Promotes the visibility of women in STEM to inspire others.
- "Community policing efforts should prioritize understanding and collaboration between law enforcement and local residents.": Solution-oriented.
- "Effective public transport systems can significantly decrease urban pollution and traffic congestion.": Solution-oriented.
- "Non-violent protest has historically been a powerful tool for social change.": Use of Examples and Evidence. Cites historical examples to illustrate the effectiveness of non-violent protest.
- "Reducing food waste is crucial for environmental sustainability and global food security.": Educational. Highlights the importance of reducing food waste for broader societal benefits.

Please classify the following text denoted in <text> as "constructive" or "not constructive".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "constructiveness": <two values, 'constructive' or 'not constructive'>,
    "reason": <optional, a 1 sentence reason for why the text is constructive>
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
"""  # noqa

rewrite_if_toxic_prompt = """
Imagine that you are a toxicity classifier. We define a post having toxicity as \
a rude, disrespectful, or unreasonable comment that is likely to make people leave a discussion. \
This content is harmful, offensive, or damaging, and is usually characterized \
by negativity, aggression, or disrespect.

Please classify the following text as "toxic" or "not toxic".

Think through your response step by step.

Then, if the post is toxic, re-write the text of a post in a way that removes \
any toxic content but maintains the original meaning and sentiment.

Return in a JSON format in the following way:
{
    "toxicity": <two values, 'toxic' or 'not toxic'>,
    "reason": <optional, a 1 sentence reason for why the text is toxic>,
    "original_text": <the original text of the post>,
    "rewritten_text": <optional, the rewritten text of the post. If not toxic, return null> 
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
"""  # noqa


task_name_to_task_prompt_map = {
    "political_ideology": political_ideology_prompt,
    "civic": civic_prompt,
    "both": civic_and_political_ideology_prompt,
    "civic_and_political_ideology": civic_and_political_ideology_prompt,
    "toxicity": toxicity_prompt,
    "constructiveness": constructiveness_prompt,
    "rewrite_if_toxic": rewrite_if_toxic_prompt,
}
