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

The Perspective API provides useful attributes for us.

### Loading the posts from MongoDB

### Classifying with the Perspective API

### Storing the results of the classifications into MongoDB

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
