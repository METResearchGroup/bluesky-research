# Using LLMs to classify if social media posts are political or not (Part VI).

I'm working on a project that involves gathering social media posts from [Bluesky](https://bsky.app/) and analyzing them. Part of that project requires knowing which posts are about political or social topics, and if so, what political side they support. Current ML classifiers don't work that well out of the box, so I'm trying to create our own classification scheme using LLMs. I'm trying to use LLMs in order to classify [Bluesky](https://bsky.app/) posts as either having political content or not, and if so, the political ideology, and I've found that LLMs work quite well for this task. I've used Llama3-8b and Llama3-70b via [Groq](https://groq.com/) so far, but are also open to experimenting with other open-source models as well (I have the on-prem infrastructure to host our own models, which is much cheaper at scale).

[Previously](https://markptorres.com/research/llm-experiments-pt-i), I confirmed that LLMs are promising for our classification task. We now want to replicate this. We [previously](https://markptorres.com/research/llm-experiments-pt-iv) synced the data for annotation. We then [cleaned up](https://markptorres.com/research/llm-experiments-pt-v) the code to have a more robust ETL pipeline.

Now we want to get some baselines with the Perspective API in order to learn more about the conversation properties of our posts.

## Primer on the Perspective API

The [Perspective API](https://perspectiveapi.com/) is an API from Google Jigsaw that detects (and reduces) toxicity online (and, as of late, also promotes healthy dialogue).

## Where the Perspective API fits into our project
If we look at the [case studies](https://perspectiveapi.com/case-studies/) of companies using the Perspective API, we see that the Perspective API works really well for content moderation in comments sections and similar longer-form text.

Our use case, however, which is analyzing posts on a social media platform, is slightly different. The actual text in a post is generally pretty short (i.e., a tweet) and multimodal. Some of the meaning can also be linked to whatever larger thread a post is embedded within, what post(s) it is responding to, external content linked, etc.

We can use the Perspective API out of the box for just classifying the text of the post, which works decently well, but there are limitations in this approach. I've tried to see if the Perspective API works on the "adding context" approach that I've been working on, but it doesn't seem to work.

## Classifiying our posts using the Perspective API

We have a thesis that by explicitly downranking toxic posts and upranking constructive posts, we'll surface more of the opinions that are traditionally silenced by algorithmic amplification and give a more nuanced perspective on what people from a particular political party believe (as opposed to just the most extreme, rage-filled, clickbait-filled posts that are normally pushed).

The Perspective API provides useful attributes for us, such as "toxicity", "constructiveness", "respect", "profanity", etc., that conveniently label conversational characteristics of the text.

### Loading the posts from MongoDB

We'll load our posts from MongoDB. A subset of our posts were directly dumped as JSON into MongoDB, whereas we transformed the schema of the most recent batch. We need to consolidate these into a single format, so I wrote a helper function to transform the old batch of posts into a consolidated format compatible with the new batch:

```python
def transform_feedviewpost_dict(post: dict) -> TransformedFeedViewPostModel:
    """Transforms a feed view post dictionary into a TransformedFeedViewPostModel.

    The transformation code from before assumes that the post is in the
    "Record" format that Bluesky has, and not a dictionary, so we'll have
    to work around those limitations.
    """
    metadata_dict: dict = post["metadata"]
    metadata_dict["url"] = post["url"]
    metadata: FeedViewPostMetadata = FeedViewPostMetadata(**metadata_dict)

    author_dict: dict = post["post"]["author"]
    author_dict_transformed = {
        "did": author_dict["did"],
        "handle": author_dict["handle"],
        "avatar": author_dict["avatar"],
        "display_name": author_dict["display_name"],
        "py_type": author_dict["py_type"]
    }
    author: TransformedProfileViewBasicModel = TransformedProfileViewBasicModel(
        **author_dict_transformed
    )

    # some fields can only be accessed when we have the old Record object.
    # instead of trying to access them or fundamentally changing our
    # transformation code to support a one-off case, we'll just use a
    # placeholder, especially since we can do analysis without these fields.
    record: dict = post["post"]["record"]
    record_dict = {
        "created_at": record["created_at"],
        "text": record["text"],
        "embed": None,
        "entities": old_format_placeholder,
        "facets": old_format_placeholder,
        "labels": old_format_placeholder,
        "langs": process_langs(record["langs"]),
        "reply_parent": old_format_placeholder,
        "reply_root": old_format_placeholder,
        "tags": process_tags(record["tags"]),
        "py_type": record["py_type"]
    }
    record: TransformedRecordModel = TransformedRecordModel(**record_dict)

    feedviewpost_dict = {
        "metadata": metadata,
        "author": author,
        "cid": post["post"]["cid"],
        "indexed_at": post["post"]["indexed_at"],
        "record": record,
        "uri": post["post"]["uri"],
        "like_count": post["post"]["like_count"],
        "reply_count": post["post"]["reply_count"],
        "repost_count": post["post"]["repost_count"]
    }
    feedviewpost: TransformedFeedViewPostModel = TransformedFeedViewPostModel(
        **feedviewpost_dict
    )
    return feedviewpost
```

Given this, we can now load our posts from MongoDB:

```python
def load_posts() -> list[TransformedFeedViewPostModel]:
    """Loads posts from the MongoDB collection."""
    posts: list[dict] = load_collection(
        collection=source_mongodb_collection, limit=None
    )
    transformed_posts = []
    # some posts won't have the proper format, but let's transform those
    # into the new format.
    for post in posts:
        try:
            transformed_posts.append(
                TransformedFeedViewPostModel(**post)
            )
        except Exception:
            transformed_feedviewpost = transform_feedviewpost_dict(post)
            transformed_posts.append(transformed_feedviewpost)
    print(f"Loaded {len(transformed_posts)} posts from MongoDB.")
    return transformed_posts
```

### Classifying with the Perspective API

First, we need to set up access to the Perspective API. That is reviewed in the [Perspective API docs](https://developers.perspectiveapi.com/s/docs?language=en_US). The Perspective API project needs to be tied to a Google Cloud account.

Once we have that set up, we'll get a Google Cloud API key. We then need access to the "commentanalyzer" client.

```python
from googleapiclient import discovery

from lib.helper import GOOGLE_API_KEY

google_client = discovery.build(
    "commentanalyzer",
    "v1alpha1",
    developerKey=GOOGLE_API_KEY,
    discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",  # noqa
    static_discovery=False,
)
```

We'll be using the following attributes:
```python
attribute_to_labels_map = {
    # production-ready attributes
    "TOXICITY": {
        "prob": "prob_toxic",
        "label": "label_toxic"
    },
    "SEVERE_TOXICITY": {
        "prob": "prob_severe_toxic",
        "label": "label_severe_toxic"
    },
    "IDENTITY_ATTACK": {
        "prob": "prob_identity_attack",
        "label": "label_identity_attack"
    },
    "INSULT": {
        "prob": "prob_insult",
        "label": "label_insult"
    },
    "PROFANITY": {
        "prob": "prob_profanity",
        "label": "label_profanity"
    },
    "THREAT": {
        "prob": "prob_threat",
        "label": "label_threat"
    },
    # constructive attributes, from Perspective API
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
    "REASONING_EXPERIMENTAL": {
        "prob": "prob_reasoning",
        "label": "label_reasoning"
    },
    "RESPECT_EXPERIMENTAL": {
        "prob": "prob_respect",
        "label": "label_respect"
    },
    # persuasion attributes
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
    "SCAPEGOATING_EXPERIMENTAL": {
        "prob": "prob_scapegoating",
        "label": "label_scapegoating"
    },
    # moderation attributes
    "SEXUALLY_EXPLICIT": {
        "prob": "prob_sexually_explicit",
        "label": "label_sexually_explicit"
    },
    "FLIRTATION": {
        "prob": "prob_flirtation",
        "label": "label_flirtation"
    },
    "SPAM": {
        "prob": "prob_spam",
        "label": "label_spam"
    },
}
```

We can then send requests to the comment analyzer. Note, this might throw errors due to the text being empty, so either catch the error (like I do here, lazily), or make sure that the text is not empty. There's some documentation and sample requests from the Perspective API docs [here](https://developers.perspectiveapi.com/s/docs-sample-requests?language=en_US) that are quite useful.
```python
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
    """  # noqa
    if not requested_attributes:
        requested_attributes = default_requested_attributes
    analyze_request = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": requested_attributes,
    }
    logger.info(
        f"Sending request to commentanalyzer endpoint with request={analyze_request}...",  # noqa
    )
    try:
        response = google_client.comments().analyze(body=analyze_request).execute()
    except HttpError as e:
        logger.error(f"Error sending request to commentanalyzer: {e}")
        response = {"error": str(e)}
    return json.dumps(response)
```

The output of a request looks like this:
```plaintext
{
  "attributeScores": {
    "TOXICITY": {
      "spanScores": [
        {
          "begin": 0,
          "end": 30,
          "score": {
            "value": 0.24173126,
            "type": "PROBABILITY"
          }
        }
      ],
      "summaryScore": {
        "value": 0.24173126,
        "type": "PROBABILITY"
      }
    }
  },
  "languages": [
    "en"
  ],
  "detectedLanguages": [
    "en"
  ]
}
```

For each of the attributes, we really only want the summary scores. We can extract that and assign a label based on the probability (as a default, I use p>0.5 = 1), which is also what some research papers that have applied the Perspective API have done. However, it seems like in production, we might want to explore higher thresholds like p>0.8 or p>0.9, according to the Perspective API docs and case studies.

```python
def classify(
    text: str, attributes: Optional[dict] = default_requested_attributes
) -> dict:
    """Classify text using all the attributes from the Google Perspectives API."""  # noqa
    response = request_comment_analyzer(
        text=text, requested_attributes=attributes
    )
    response_obj = json.loads(response)
    if "error" in response_obj:
        return {"error": response_obj["error"]}
    classification_probs_and_labels = {}
    for attribute, labels in attribute_to_labels_map.items():
        if attribute in response_obj["attributeScores"]:
            prob_score = (
                response_obj["attributeScores"][attribute]["summaryScore"]["value"]  # noqa
            )
            classification_probs_and_labels[labels["prob"]] = prob_score
            classification_probs_and_labels[labels["label"]
                                            ] = 0 if prob_score < 0.5 else 1
    return classification_probs_and_labels
```

We can then run this on our posts:

```python
def classify_post(
    post: TransformedFeedViewPostModel
) -> PerspectiveAPIClassification:
    """Classifies post with the Perspective API.

    We'll set the text to just be the text of the post itself, even if it
    doesn't have much text. We can do filtering as part of postprocessing.
    """
    uri = post.uri
    text = post.record.text
    classifications: dict = classify(text=text)
    return PerspectiveAPIClassification(
        uri=uri,
        text=text,
        classifications=classifications
    )


def classify_posts(posts: list[dict]) -> list[PerspectiveAPIClassification]:
    classifications = [
        classify_post(post) for post in posts
    ]
    return classifications
```

### Storing the results of the classifications into MongoDB

Now that we have the classifications, we can store the posts.

```python
def export_posts_local(posts: list[dict]):
    print(f"Writing {len(posts)} posts to {full_label_fp}")
    with open(full_label_fp, "w") as f:
        for post in posts:
            post_json = json.dumps(post)
            f.write(post_json + "\n")
    num_posts = len(posts)
    print(f"Wrote {num_posts} posts locally to {full_label_fp}")
    pass


def export_posts_remote(posts: list[dict]):
    duplicate_key_count = 0
    total_successful_inserts = 0
    total_posts = len(posts)
    print(f"Inserting {total_posts} posts to MongoDB collection {label_collection_name}")  # noqa
    formatted_posts_mongodb = [
        {"_id": post["uri"], **post}
        for post in posts
    ]
    print("Inserting into MongoDB in bulk...")
    total_successful_inserts, duplicate_key_count = chunk_insert_posts(
        posts=formatted_posts_mongodb,
        mongo_collection=label_mongodb_collection,
        chunk_size=DEFAULT_INSERT_CHUNK_SIZE
    )
    if duplicate_key_count > 0:
        print(f"Skipped {duplicate_key_count} duplicate posts")
    print(f"Inserted {total_successful_inserts} posts to remote MongoDB collection {label_collection_name}")  # noqa
    print("Finished bulk inserting into MongoDB.")


def export_classified_posts(
    classified_posts: list[PerspectiveAPIClassification],
    store_local: bool = True,
    store_remote: bool = True
):
    """Exports the classified posts."""
    posts: list[dict] = [post.dict() for post in classified_posts]
    if store_local:
        export_posts_local(posts)
    if store_remote:
        export_posts_remote(posts)
```

We can now run the classification and see how it looks:
```python
@track_performance
def main():
    classified_posts: list[PerspectiveAPIClassification] = load_and_classify_posts()  # noqa
    kwargs = {
        "classified_posts": classified_posts,
        "store_local": True,
        "store_remote": True
    }
    export_classified_posts(**kwargs)
```

```plaintext
Inserting 2736 posts to MongoDB collection bluesky_posts_perspective_api_labels
Inserting into MongoDB in bulk...
Inserted 2736 posts to remote MongoDB collection bluesky_posts_perspective_api_labels
Finished bulk inserting into MongoDB.
Execution time for main: 6 minutes, 20 seconds
Memory usage for main: 28.1875 MB
```

We classified 2,736 posts using the Perspective API. It took 6 minutes, although that was only because I used a for-loop for classifications (it truly didn't have to take that long, but I just wanted to have something that worked for now).

## Experiments and analysis with the Perspective API

### What do the distributions of our labels look like?

### How do different labels correlate?

### Let's look at some examples and see if they make sense

## Hand-labeling which samples to uprank/downrank

## Creating a demo app with Streamlit for future analysis

## Next steps
Now that we have some benchmarks for ...

Some of the things that I want to work on next are:
- Updating and refactoring the LLM pipeline to label the posts efficiently at scale.
- How does our model perform with other LLMs (e.g., Mixtral)?
- Can we experiment with optimizing the prompt (e.g, with [dspy](https://github.com/stanfordnlp/dspy))?

I'd also like to revisit some of the points related to improving how to add context about current events:
- For determining when to get context for a post, investigate various strategies such as:
    - Keyword matching: see if a keyword (e.g., a name of an event) comes up. Need to figure out keywords that describe topics that are in the news (this is easiest if it is the name of a notable event, place, person, piece of legislature, etc.) and then we can easily pattern match that against posts that have that keyword.
    - Posts that the LLM knows is political but isn't sure what the political ideology is.
- Determine how to format the context that's given to the LLM prompt.
    - An interesting frame could be first asking the LLM to distill the sentiments and thoughts of each political party about a certain topic, based on the articles that we have for each topic, and then passing this distilled summary to the LLM itself.
- Only insert into the vector store if it doesn’t already exist there.
- At some point, add a maximum distance measure so we get only relevant articles (will take some experimentation in order to see what a good distance is).
