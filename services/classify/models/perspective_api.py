import json
import time
from typing import Optional

from googleapiclient import discovery

from lib.helper import GOOGLE_API_KEY, logger

google_client = discovery.build(
  "commentanalyzer",
  "v1alpha1",
  developerKey=GOOGLE_API_KEY,
  discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
  static_discovery=False,
)

default_requested_attributes = {
    "TOXICITY": {},
    # constructive attributes, from Perspective API
    "AFFINITY_EXPERIMENTAL": {},
    "COMPASSION_EXPERIMENTAL": {},
    "CONSTRUCTIVE_EXPERIMENTAL": {},
    "CURIOSITY_EXPERIMENTAL": {},
    "NUANCE_EXPERIMENTAL": {},
    "PERSONAL_STORY_EXPERIMENTAL": {},
    # persuasion attributes, from Perspective API
    "ALIENATION_EXPERIMENTAL": {},
    "FEARMONGERING_EXPERIMENTAL": {},
    "GENERALIZATION_EXPERIMENTAL": {},
    "MORAL_OUTRAGE_EXPERIMENTAL": {},
    #"POWER_APPEAL_EXPERIMENTAL": {},
    "SCAPEGOATING_EXPERIMENTAL": {}
}

attribute_to_labels_map = {
    "TOXICITY": {
        "prob": "prob_toxic",
        "label": "label_toxic"
    },
    "AFFINITY_EXPERIMENTAL": {
        "prob": "prob_affinity",
        "label": "label_affinity"
    },
    "COMPASSION_EXPERIMENTAL": {
        "prob": "prob_compassion",
        "label": "label_compassion"
    },
    "CONSTRUCTIVE_EXPERIMENTAL": {
        "prob": "prob_constructive",
        "label": "label_constructive"
    },
    "CURIOSITY_EXPERIMENTAL": {
        "prob": "prob_curiosity",
        "label": "label_curiosity"
    },
    "NUANCE_EXPERIMENTAL": {
        "prob": "prob_nuance",
        "label": "label_nuance"
    },
    "PERSONAL_STORY_EXPERIMENTAL": {
        "prob": "prob_personal_story",
        "label": "label_personal_story"
    },
    "ALIENATION_EXPERIMENTAL": {
        "prob": "prob_alienation",
        "label": "label_alienation"
    },
    "FEARMONGERING_EXPERIMENTAL": {
        "prob": "prob_fearmongering",
        "label": "label_fearmongering"
    },
    "GENERALIZATION_EXPERIMENTAL": {
        "prob": "prob_generalization",
        "label": "label_generalization"
    },
    "MORAL_OUTRAGE_EXPERIMENTAL": {
        "prob": "prob_moral_outrage",
        "label": "label_moral_outrage"
    },
    "POWER_APPEAL_EXPERIMENTAL": {
        "prob": "prob_power_appeal",
        "label": "label_power_appeal"
    },
    "SCAPEGOATING_EXPERIMENTAL": {
        "prob": "prob_scapegoating",
        "label": "label_scapegoating"
    }
}

def request_comment_analyzer(
    text: str, requested_attributes: dict = None
) -> dict:
    """Sends request to commentanalyzer endpoint.

    Docs at https://developers.perspectiveapi.com/s/docs-sample-requests?language=en_US

    Example request:

    analyze_request = {
    'comment': { 'text': 'friendly greetings from python' },
    'requestedAttributes': {'TOXICITY': {}}
    }

    response = client.comments().analyze(body=analyze_request).execute()
    print(json.dumps(response, indent=2))
    """
    if not requested_attributes:
        requested_attributes = default_requested_attributes
    analyze_request = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": requested_attributes,
    }
    logger.info(
        f"Sending request to commentanalyzer endpoint with request={analyze_request}...", # noqa
    )
    response = google_client.comments().analyze(body=analyze_request).execute()
    return json.dumps(response)


def classify_text_toxicity(text: str) -> dict:
    """Classify text toxicity."""
    response = request_comment_analyzer(
        text=text, requested_attributes={"TOXICITY": {}}
    )
    response_obj = json.loads(response)
    toxicity_prob_score = (
        response_obj["attributeScores"]["TOXICITY"]["summaryScore"]["value"]
    )

    return {
        "prob_toxic": toxicity_prob_score,
        "label_toxic": 0 if toxicity_prob_score < 0.5 else 1
    }


def classify_text(
    text: str,
    attributes: Optional[dict] = default_requested_attributes
  ) -> dict:
    """Classify text using all the attributes from the Google Perspectives API.""" # noqa
    response = request_comment_analyzer(
        text=text, requested_attributes=attributes
    )
    response_obj = json.loads(response)
    classification_probs_and_labels = {}
    for attribute, labels in attribute_to_labels_map.items():
        if attribute in response_obj["attributeScores"]:
          prob_score = (
              response_obj["attributeScores"][attribute]["summaryScore"]["value"]
          )
          classification_probs_and_labels[labels["prob"]] = prob_score
          classification_probs_and_labels[labels["label"]] = 0 if prob_score < 0.5 else 1
    return classification_probs_and_labels


def perform_batch_inference(batch: list[str]) -> list[dict]:
    """Performs inference using the Google Perspectives API.

    Returns a list of JSONs of the following format:
    {
        "text": <text>,
        "TOXICITY": <binary 0 or 1>,
        "AFFINITY_EXPERIMENTAL": <binary 0 or 1>,
        "COMPASSION_EXPERIMENTAL": <binary 0 or 1>,
        "CONSTRUCTIVE_EXPERIMENTAL": <binary 0 or 1>,
        "CURIOSITY_EXPERIMENTAL": <binary 0 or 1>,
        "NUANCE_EXPERIMENTAL": <binary 0 or 1>,
        "PERSONAL_STORY_EXPERIMENTAL": <binary 0 or 1>,
        "ALIENATION_EXPERIMENTAL": <binary 0 or 1>,
        "FEARMONGERING_EXPERIMENTAL": <binary 0 or 1>,
        "GENERALIZATION_EXPERIMENTAL": <binary 0 or 1>,
        "MORAL_OUTRAGE_EXPERIMENTAL": <binary 0 or 1>,
        "POWER_APPEAL_EXPERIMENTAL": <binary 0 or 1>,
        "SCAPEGOATING_EXPERIMENTAL": <binary 0 or 1>
    }
    """
    processed_posts = []
    for idx, text in enumerate(batch):
        if idx % 30 == 0:
            logger.info(f"Processed {idx} posts...")
        classification_probs_and_labels = classify_text(text)
        processed_post = {
            "text": text,
            **classification_probs_and_labels
        }
        processed_posts.append(processed_post)
        time.sleep(1.2)
    return processed_posts


if __name__ == "__main__":
    texts = [
        "Restricting abortion access in all cases is morally wrong.",
        "The situation in Gaza is a horrible humanitarian crisis and we shouldn't ignore it.",
        "I can't believe the refs are so biased in the Super Bowl!"
    ]

    """
    texts = [
        "People who voted for Trump are dumb.",
        "People who voted for Biden are dumb.",
        "People who voted for Trump are brainwashed.",
        "People who voted for Biden are brainwashed.",
        "People who voted for Trump are misinformed.",
        "People who voted for Biden are misinformed.",
        "People who voted for Trump are evil.",
        "People who voted for Biden are evil.",
        "People who voted for Trump are good.",
        "People who voted for Biden are good.",
        "People who voted for Trump are right.",
        "People who voted for Biden are right.",
        "People who voted for Trump are wrong.",
        "People who voted for Biden are wrong.",
        "People who voted for Trump do not know their history",
        "People who voted for Biden do not know their history",
        "People who voted for Trump are not well-educated",
        "People who voted for Biden are not well-educated"
    ]
    """
    texts = [
        "People who vote for Trump have to educate themselves", 
        # https://twitter.com/BriannaWu/status/1761041178504384645
        # expected: not toxic, people won't get upset in the comments section
        """
Respectfully, let's just go through some history. 

In 2000, Palestinians walk away from a very, very good peace deal which included a sovereign state (a “Free Palestine,” if you will.)

They then launch the Second Intifada. Israeli civilians are living though five years of suicide bombings, mass shootings and rocket attacks. This massively deteriorated any chance at peace. 

We both agree that Netanyahu has been horrible. But it's simply factual that Palestinians created the conditions that led to his rise.
        """,
        # https://twitter.com/Adapa69/status/1761036540992803312
        # expected: toxic, not constructive
        """If Israel has to be an apartheid state to exist.. then it should be destroyed. The fact the Bibi and his party keep getting re-elected means "the only democracy in the middle east" is populated with a majority that are evil fucks.""",
        # https://twitter.com/bribri923/status/1761042117563240752
        # expected: toxic, not constructive
        "What do you suggest they do about the part where Palestinian leadership's only goal is to destroy it?  We just ignore that?",
        # https://twitter.com/beemerw21/status/1761033473299542512
        # expected: this looks productive. Comments section doesn't have vitriol, but are good points.
        # the post itself communicates a moral stance without removing emotions.
        "I think a lot of non-Jews don't quite comprehend the Jewish people's emotional connection to Israel. We can condemn its government's actions but its existence is a core, or at least spiritual part of our religious identity. So we can't just... give it up.",
        # https://twitter.com/SpencerJJoseph/status/1761068555217318181
        # expected: can be biased. 
        "There is a very strong *legal* & ethnic connection that should also be emphasised. The State of Israel is the result of international agreement and binding statutes of law following UN Resolution 181 in 1947. Land was legally purchased and worked by Jews prior to this date.",
        # https://twitter.com/SpencerJJoseph/status/1761126511887695903
        # expected: toxic, not constructive
        "This isn't racist elementary school. Open some books, read and find out why you know nothing about this subject."
        # https://twitter.com/alciblades1234/status/1761203861182644303
        # expected: this is biased and a hypothetical question that isn't a very
        # constructive question.
        "What good are those laws if the conflict continues anyway? What good are those laws if we constantly have endless terrorism and arguments about the settlements and such?",
        # https://twitter.com/web2scholar/status/1761204349001257392
        # expected: Opinionated and outraged, but not toxic.
        "They forgot to ask the Palestinians. You know.. the only people whose opinion mattered when they decided"
    ]

    texts = [
        # toxic + compassionate
        "The way you're dismissing valid concerns is infuriating, but I get that it's a stressful time and we're all on edge",
        # toxic + constructive
        "Your understanding of the electoral college is embarrassingly wrong, but I'm posting a link to help educate you on it.",
        # toxic + constructive
        "Stop spreading misinformation about voter fraud; here are the actual stats and figures from reliable sources",
        # toxic + constructive
        "It seems you didn't even watch the full debate. Before you argue, please watch the entire thing and pay attention to the key points",
        # toxic + constructive
        "Honestly, your post misses the mark by a wide margin, but let's discuss it point by point to see where the misunderstanding is.",
        # toxic + compassionate
        "It's baffling that you still hold onto these views, yet I can see it's important to you, and I respect your passion for the subject.",
        # toxic + compassionate
        "It's absurd to think your vote doesn't matter, but I know a lot of people feel disenfranchised, and that's a real issue we need to address."
    ]

    texts_comments_map_list = [
        {
            "text": "The way you're dismissing valid concerns is infuriating, but I get that it's a stressful time and we're all on edge",
            "comment": "toxic + compassionate"
        },
        {
            "text":  "Your understanding of the electoral college is embarrassingly wrong, but I'm posting a link to help educate you on it.",
            "comment": "toxic + constructive"
        },
        {
            "text": "Stop spreading misinformation about voter fraud; here are the actual stats and figures from reliable sources",
            "comment": "toxic + constructive"
        },
        {
            "text": "It seems you didn't even watch the full debate. Before you argue, please watch the entire thing and pay attention to the key points",
            "comment": "toxic + constructive"
        },
        {
            "text": "Honestly, your post misses the mark by a wide margin, but let's discuss it point by point to see where the misunderstanding is.",
            "comment": "toxic + constructive"
        },
        {
            "text": "It's baffling that you still hold onto these views, yet I can see it's important to you, and I respect your passion for the subject.",
            "comment": "toxic + compassionate"
        },
        {
            "text": "It's absurd to think your vote doesn't matter, but I know a lot of people feel disenfranchised, and that's a real issue we need to address.",
            "comment": "toxic + compassionate"
        },
        {
            "text": "That's the most ridiculous take I've heard on the election results, but I understand emotions are running high, and it's a stressful time for everyone",
            "comment": "toxic + compassionate"
        },
        {
            "text": "Your stance seems ill-informed and hasty, but I understand that it's a complex topic, and we all miss details sometimes.",
            "comment": "toxic + compassionate"
        },
        {
            "text": "This is the same tired argument we've seen before, and it's getting old. Nonetheless, I've taken the time to lay out why it doesn't hold up.",
            "comment": "toxic + constructive"
        },
        {
            "text": "You're spreading common misconceptions, which isn't helping anyone. It's a complex issue, though, so let's try to unpack it together.",
            "comment": "toxic + compassionate"   
        },
        {
            "text": "It seems like you're choosing to ignore the facts, which is hard to watch. If you're open to it, I can share some resources that might change your perspective.",
            "comment": "toxic + compassionate"
        },
        {
            "text": "That's a really narrow way of thinking about such a serious issue, but I get that it's a response to being overwhelmed. Let's try to broaden our view.",
            "comment": "toxic + compassionate"
        },
        {
            "text": "Claiming that nothing can be done is a defeatist attitude, and it's sad to see. However, I believe change is possible with effort, and I'm here to support that journey.",
            "comment": "toxic + compassionate"
        }
    ]

    texts_comments_map_list = [
        {
            "text": "People who voted for Trump are dumb.",
            "comment": "toxic"
        },
        {
            "text": "People who voted for Trump are dumb, but it's not their fault.",
            "comment": "toxic + compassionate?"
        },
        {
            "text": "People who voted for Trump are dumb, but it's not their fault. We should fix the education system in order to prevent this from happening again.",
            "comment": "toxic + compassionate + constructive?"
        },
        {
            "text": "People who voted for Trump are dumb, but it's not their fault, they've been brainwashed.",
            "comment": "toxic + compassionate + alienation?"
        },
        {
            "text": "People who voted for Trump are dumb. This is a result of a terrible misinformation machine, and we need to lobby for fake news to be outlawed",
            "comment": "toxic + constructive?"
        }
    ]

    texts_comments_map_list = [
        {
            "text": "Fans of the Chiefs are the worst human beings, they're such bandwagons and don't know real football when they see it.",
            "comment": "toxic but non-civil, non-political"
        },
        {
            "text": "The latest updates to Google Gemini just show the incompetence of Google and the arrogance they have. How could they let out something so unpolished?",
            "comment": "toxic but non-civil, non-political"
        },
        {
            "text": "The situation in Ukraine is terrible and I hate Putin for what he's doing. The US government is complicit in genocide if they let this keep going. Those Republicans blocking the bills are Nazi pigs.",
            "comment": "toxic, civill, political"
        },
        {
            "text": "The 2020 election was rigged and I can't believe the Democrat witch-hunt is still ongoing. Biden and his stupid corrupt cronies will all be jailed once Trump is re-elected.",
            "comment": "toxic, civil, political"
        }
    ]


    # fact vs. emotion? More facts in a post, less likely to be toxic?
    texts = [obj["text"] for obj in texts_comments_map_list]

    texts = [
        "Twitter is nuking every single post that mentions the name Hans Kristian Graebener, even in quotes. Everyone that posts it is getting hit. I've never seen sitewide censorship like this done specifically on behalf of a neo nazi.",
        "I've never seen sitewide censorship like this done specifically on behalf of a neo nazi.",
        "Every time. UBI works. AI doesn't. But which one is getting billions in hot dumb money thrown at it.",
        "The reason we need to keep talking about this is that it's not about him being a hypocrite. It's about the fact that a major social media platform has turned into a propaganda arm of the fascist right and that freedom of speech and press is under attack on this platform and beyond.",
        "It's about the fact that a major social media platform has turned into a propaganda arm of the fascist right and that freedom of speech and press is under attack on this platform and beyond.",
        "Watching Trump hold rallies where he incites violence after January 6th WHILE OUT ON BAIL is like watching bin Laden teach a flight school after 9/11."
    ]

    outputs: list[dict] = perform_batch_inference(texts)

    breakpoint()

    outputs = [
        {
            **output,
            **obj
        }
        for output, obj in zip(outputs, texts_comments_map_list)
    ]
    # TODO: we can set our own thresholds for probabilities. For example, 
    # "The situation in Gaza is a horrible humanitarian crisis and we shouldn't ignore it.",
    # should probably be compassion, but it only has p=0.40, so we classify it as
    # not having compassion (label=0) with our default threshold of 0.5.
    import pandas as pd
    df = pd.DataFrame(outputs)
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d-%H-%M-%S")
    df.to_csv(f"perspective_api_outputs_{timestamp}.csv", index=False)
    breakpoint()
