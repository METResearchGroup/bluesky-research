# Using LLMs to classify if social media posts are political or not (Part I).

I'm working on a project that involves gathering social media posts from [Bluesky](https://bsky.app/) and analyzing them. Part of that project requires knowing which posts are about political or social topics, and if so, what political side they support. Current ML classifiers don't work that well out of the box, so I'm trying to create our own classification scheme using LLMs. I'm trying to use LLMs in order to classify [Bluesky](https://bsky.app/) posts as either having political content or not, and if so, the political ideology, and I've found that LLMs work quite well for this task. I've used Llama3-8b and Llama3-70b via [Groq](https://groq.com/) so far, but are also open to experimenting with other open-source models as well (I have the on-prem infrastructure to host our own models, which is much cheaper at scale).

## The evolution of our approach

### Attempt 1: Naive text classification

At first, I tried the naive, obvious approach, which is to just take the raw text of the post and ask the LLM to classify it. I started with a prompt as simple as something like this:

```{plaintext}
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text denoted in <text> as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "democrat", "republican", or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. \
If the text is not sociopolitical, return "unclear".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "sociopolitical": <two values, 'sociopolitical' or 'not sociopolitical'>,
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical. If none, return an empty string, "">,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology. If none, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.
```

For many political posts, like the following, this approach actually works decently well:
![Political Bluesky post](assets/images/political_post_plain.png "Political Bluesky post") 

Here is the prompt to the LLM for that post:
```plaintext
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text denoted in <text> as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "democrat", "republican", or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. \
If the text is not sociopolitical, return "unclear".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "sociopolitical": <two values, 'sociopolitical' or 'not sociopolitical'>,
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical. If none, return an empty string, "">,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology. If none, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.

<text>
Can’t think of a more American moment than “now that I won $200 million, I can finally afford decent healthcare.”
```

Here's what the LLM said:
```python
{
    "sociopolitical": "sociopolitical",
    "political_ideology": "democrat",
    "reason_sociopolitical": "The text discusses healthcare affordability, a major social issue that affects a large group of people.",
    "reason_political_ideology": "The statement critiques healthcare affordability, aligning with views typically associated with Democratic perspectives on healthcare reform."
}
```

It also is good for the majority of cases that don't discuss political content:
![Non-political Bluesky post](assets/images/not_political_post.png "Non-political Bluesky post") 

Here's what the LLM said:
```python
{
    "sociopolitical": "not sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "",
    "reason_political_ideology": ""
}
```

I've tried this approach, but it ends up failing on posts like this:
![Example Bluesky post](assets/images/sample_post_no_context.png "Example Bluesky post")

If we ask this to the LLM, we get the following response:
```python
{
    "sociopolitical": "not sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "",
    "reason_political_ideology": ""
}
```

If, however, we look at the entire post, we can see that there's context that we're missing, specifically the wider thread that it is a part of:
![Example Bluesky post (with thread context)](assets/images/sample_post_context.png "Example Bluesky post (with thread context)")

However, looking at just the post text itself isn't enough. If we look at the thread that the post is a part of, we as humans can clearly tell this post is replying to another post and, if we know what post it's replying to, we can clearly tell that it's political and left-leaning.

For these posts, the text in and of itself inherently is insufficient. Here's some more examples that the LLM would've classified incorrectly:

![Example Bluesky post without context](assets/images/sample_post_no_context_2.png "Example Bluesky post without context")
![Example Bluesky post without context](assets/images/sample_post_no_context_3.png "Example Bluesky post without context")

Just by looking at the text of the post themselves, it would've been impossible for the LLM to classify these correctly. For the first image, the LLM doesn't have the appropriate knowledge cutoff to classify this correct:

```python

{
    "sociopolitical": "sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "The text mentions political dynamics involving job retention and loss due to interactions with politicians.",
    "reason_political_ideology": "The text does not provide enough information to determine a specific political ideology or lean."
}
```

For the second image, the meaning of the post is within the image itself, so the LLM not knowing what's in the image makes it unable to classify the image correctly:

```python
{
    "sociopolitical": "not sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "",
    "reason_political_ideology": ""
}
```

What would have helped the LLM classify these posts correctly? I'm using the LLM here as a large comprehension machine, but if I want the LLM to work well, I need to give it the same info that I would give to a person who's unfamiliar with US politics if I were to ask them to classify this same task themselves. After all, for example, if a post references current events that are after the knowledge cutoff of the LLM (very likely given how quickly news cycles move), we'll need to supply that knowledge ourselves.

This tells us that a way to improve our approach towards classification is to also include context as well about the post. We're not going to get all the meaning of a post from just the text itself, and as humans we also don't look at just the text of a post to get its meaning anyways. Logically, we should be giving the same information to the LLM that we ourselves would want to use in order to classify this task.

### Attempt 2: Adding context to the text


If we take our previous example:
![Example Bluesky post](assets/images/sample_post_no_context.png "Example Bluesky post")

whose previous classification without context is
```python
{
    "sociopolitical": "not sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "",
    "reason_political_ideology": ""
}
```

we can see that by adding extra information such as the thread that the post is a part of, we would be able to change the LLM's classification. 

### Initial ideas for possible context to add

To start, I added the following ways of including context:
- **Content referenced**: does the post link to another post? Does the post have an image (with alt-text; I don't think it's practical nor necessary to include OCR to grab the text of any images)? Does the post link to an external link (i.e., a news article?).
- **URLs**: does the post link to any outside content or URLs?
- **Thread that a post is part of**: if the post is a part of a thread? If so, what is the parent post that this post is responding to? What is the first post in the thread?
- **Tags and labels in the post**: has the post author added any hashtags or labels onto their post?
- **Context about current events**: the purpose of this is to give the model more knowledge about current events. We'll be unable to 
- **Context about the post author**: for example, is the author a reputable news org.

My initial testing didn't include current events context, since that'll be a bit more involved and will likely involve some RAG-based retrieval system.

### Example prompts using context

We can look at some examples of prompts and see how it's changed with context. For example, we can look at one of our example posts:
![Example Bluesky post](assets/images/sample_post_no_context.png "Example Bluesky post")

Here's the prompt without context:

```plaintext
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text denoted in <text> as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "democrat", "republican", or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. \
If the text is not sociopolitical, return "unclear".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "sociopolitical": <two values, 'sociopolitical' or 'not sociopolitical'>,
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical. If none, return an empty string, "">,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology. If none, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.

<text>
Curious what it looks like when people are aggressively not moving.
```

Here's the prompt with context:

```plaintext
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text denoted in <text> as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "democrat", "republican", or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. \
If the text is not sociopolitical, return "unclear".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "sociopolitical": <two values, 'sociopolitical' or 'not sociopolitical'>,
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical. If none, return an empty string, "">,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology. If none, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.

<text>
Curious what it looks like when people are aggressively not moving.

Here is the post text that needs to be classified:
'''
<text>
Curious what it looks like when people are aggressively not moving.
'''
Here is some context on the post that needs classification:
'''
<Thread that the post is a part of>

The post is a reply to another post with the following details:
'''
[text]: A high ranking NYPD official characterized NYU faculty linking hands and refusing to move—a classic tactic of *peaceful* protest—as aggression toward its officers and he got it uncritically published by the local Fox station and every outlet that aggregated from them. wapo.st/3UwN0Vf 🎁
'''
'''
Again, the text of the post that needs to be classified is:
'''
<text>
Curious what it looks like when people are aggressively not moving.
```

This causes our classification to change. Here is the original:
```python
{
    "sociopolitical": "not sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "",
    "reason_political_ideology": ""
}
```


Here is the new classification:

```python

{
    "sociopolitical": "sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "The post discusses a peaceful protest involving NYU faculty, indicating sociopolitical content related to activism.",
    "reason_political_ideology": "The text does not provide clear information regarding a specific political ideology."
}
```

The LLM does identify the post as sociopolitical, but it can't classify the ideology. This is a case where additional current events context would help.

Here is another example of a post:
![Example Bluesky post without context](assets/images/sample_post_no_context_2.png "Example Bluesky post without context")

This is the prompt for the post:
```
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers \
to whether a given post is related to politics (government, elections, politicians, activism, etc.) or \
social issues (major issues that affect a large group of people, such as the economy, inequality, \
racism, education, immigration, human rights, the environment, etc.). We refer to any content \
that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. \
Please classify the following text denoted in <text> as "sociopolitical" or "not sociopolitical". 

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument \
it presents. Your options are "democrat", "republican", or 'unclear'. \
You are analyzing text that has been pre-identified as 'political' in nature. \
If the text is not sociopolitical, return "unclear".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "sociopolitical": <two values, 'sociopolitical' or 'not sociopolitical'>,
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical. If none, return an empty string, "">,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology. If none, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.

Here is the post text that needs to be classified:
'''
<text>
Time for Shafiq to go. Ironic her cowing to unappeasable pols to keep her job actually led to her losing it. And deserved too.
'''


Here is some context on the post that needs classification:
'''
<Thread that the post is a part of>

The post is a reply to another post with the following details:
'''
[text]: Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence.
'''


'''
Again, the text of the post that needs to be classified is:
'''
<text>
Time for Shafiq to go. Ironic her cowing to unappeasable pols to keep her job actually led to her losing it. And deserved too.
'''
```

Here is the original classification:
```

```

Here is the updated classification after adding context:
```python
{
    "sociopolitical": "sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "The text discusses the consequences of political actions and decisions in a university setting, relating to leadership and governance.",
    "reason_political_ideology": "The text does not specify any political ideology or alignment associated with the actions or opinions expressed."
}
```

Like in the LLM does identify the post as sociopolitical, but it can't classify the ideology. This is a case where additional current events context would help.

At the very least, we do see promise with using this approach. It is clear that adding context and giving the LLM more information to work with is a viable approach to improve its classification of political content.

### Empirical testing using Llama3

Now that we have prompts that work, let's start testing them.

I'd like to test using Llama3, since we'll eventually host the weights on-premises in a HPC. I could theoretically download Llama3 locally, e.g., via [Llama3.cpp](https://github.com/ggerganov/llama.cpp), but conveniently, [Groq](https://groq.com/) lets us do inference [quickly](https://wow.groq.com/lpu-inference-engine/) and with a [free tier](https://console.groq.com/settings/limits), so I'll use that. I'll also use [LiteLLM](https://www.litellm.ai/), which is a nice abstraction layer that lets you use a variety of LLM backends using the same OpenAI format. They conveniently support [Groq](https://litellm.vercel.app/docs/providers/groq), so I'll use that.

For the model settings, I used `temperature=0.0` so we could get as deterministic results as possible. We can also add `"response_format": {"type": "json_object"}` in order to force the model to return a JSON object - it will fail if unable to return a JSON.


```python
import os

os.environ["GROQ_API_KEY"] = "<GROQ_API_KEY>"


BACKEND_OPTIONS = {
    "Llama3-8b (via Groq)": {
        "model": "groq/llama3-8b-8192",
        "kwargs": {
            "temperature": 0.0,
            "response_format": {"type": "json_object"},  # https://console.groq.com/docs/text-chat#json-mode
        }
    },
    "Llama3-70b (via Groq)": {
        "model": "groq/llama3-70b-8192",
        "kwargs": {
            "temperature": 0.0,
            "response_format": {"type": "json_object"},  # https://console.groq.com/docs/text-chat#json-mode
        }
    },
}
```

I built a wrapper around the LiteLLM interface in order to run a query:


```python
from litellm import completion
from litellm.utils import ModelResponse

def run_query(
    prompt: str, role: str = "user", model_name: str = "Llama3-8b (via Groq)"
) -> str:
    """Runs a query to an LLM model and returns the response."""
    model_dict = BACKEND_OPTIONS[model_name]
    model_params = model_dict.copy()
    kwargs = {
        "model": model_params.pop("model"),
        "messages": [{"role": role, "content": prompt}],
        **model_params["kwargs"]
    }
    response: ModelResponse = completion(**kwargs)
    content: str = (
        response.get('choices', [{}])[0].get('message', {}).get('content')
    )
    return content
```

I then loaded a [previously hand-labeled](https://github.com/METResearchGroup/bluesky-research/blob/main/demos/manuscript_pilot/hand_labeled_pilot_posts.csv) set of pilot data to use for this project. Now we have all the pieces necessary to actually run the labeling and testing.

### Results from Llama3 testing

*sidenote: I previously defined "sociopolitical" as "civic', so the graphs below will use "civic".*

#### How does adding context change performance?

I also tested how adding context changes the performance of each model.

###### Llama3-8b

Civic/sociopolitical classification:

Here are the raw counts.

| Metric       | Llama3-8b (no context) | Llama3-8b (with context) |
|--------------|-------------------------|----------------------------|
| Total civic  | 114                     | 141                        |
| Total non-civic | 88                   | 61                         |

Here are those counts, as proportions.

| Metric    | Llama3-8b (no context) | Llama3-8b (with context) |
|-----------|-------------------------|----------------------------|
| Accuracy  | 0.8317                  | 0.8267                     |
| Precision | 0.9196                  | 0.8298                     |
| Recall    | 0.8047                  | 0.9141                     |
| F1-score  | 0.8585                  | 0.8699                     |


Political ideology classification:

Here are the raw counts.

| Metric             | Llama3-8b (no context) | Llama3-8b (with context) |
|--------------------|------------------------|--------------------------|
| Total left-leaning | 73                     | 76                       |
| Total moderate     | 2                      | 5                        |
| Total right-leaning| 11                     | 14                       |
| Total unclear      | 17                     | 22                       |

Here are those counts, as proportions.

| Metric    | Llama3-8b (no context) | Llama3-8b (with context) |
|-----------|------------------------|--------------------------|
| Accuracy  | 0.6990                 | 0.6923                   |
| Precision | 0.7582                 | 0.7809                   |
| Recall    | 0.6990                 | 0.6923                   |
| F1-score  | 0.7196                 | 0.7237                   |


**Takeaway** For Llama3-8b, adding context doesn't make much much of a difference. At best, adding context slightly improves the F1 score. It seems like adding context allows the LLM to perform better at classifying sociopolitical posts, but there's not much noticeable different in terms of political ideology classification.

###### Llama3-70b

Civic/sociopolitical classification:

Here are the raw counts.

| Metric       | Llama3-70b (no context) | Llama3-70b (with context) |
|--------------|-------------------------|---------------------------|
| Total civic  | 113                     | 144                       |
| Total non-civic | 90                   | 59                        |

Here are those counts, as proportions.

| Metric     | Llama3-70b (no context) | Llama3-70b (with context) | Net change | Percent change |
|------------|-------------------------|---------------------------|------------|----------------|
| Accuracy   | 0.8522                  | 0.9064                    | 0.0542     | +6.4%          |
| Precision  | 0.9381                  | 0.8819                    | -0.0562    | -6.0%          |
| Recall     | 0.8217                  | 0.9845                    | 0.1628     | +19.8%         |
| F1-score   | 0.9013                  | 0.9304                    | 0.0291     | +3.23%         |



Political ideology classification:

For political ideology, I calculated metrics and counts on posts that were both classified as civic by the model and were actually civic.

Here are the raw counts.

| Metric            | Llama3-70b (no context) | Llama3-70b (with context) |
|-------------------|-------------------------|---------------------------|
| Total left-leaning| 90                      | 107                       |
| Total moderate    | 6                       | 8                         |
| Total right-leaning | 5                     | 7                         |
| Total unclear     | 5                       | 5                         |

Here are those counts, as proportions.

| Metric     | Llama3-70b (no context) | Llama3-70b (with context) | Percent change |
|------------|-------------------------|---------------------------|----------------|
| Accuracy   | 0.8208                  | 0.8504                    | +3.61%         |
| Precision  | 0.7937                  | 0.8611                    | +8.49%         |
| Recall     | 0.8208                  | 0.8504                    | +3.61%         |
| F1-score   | 0.8002                  | 0.8356                    | +4.42%         |

**Takeaway** Adding context makes a significant difference in the larger model - we see a huge jump in performance for both civic/sociopolitical and political ideology classification when adding context. The classification of sociopolitical posts is impressive in particular - given the constraints of the annotation process (lack of context, me giving inconsistent labels, etc.), the maximum recall that I would've estimated was possible was ~90%. Here, the LLM, with context, gets over 90% accuracy (and 0.98 recall!), which is quite impressive.


##### How does the performance vary by model size?

We can see how performance varies by model size:

For sociopolitical/civic classification:

| Metric (civic) | Llama3-8b* (with context) | Llama3-70b (with context) | Percent change |
|----------------|---------------------------|---------------------------|----------------|
| Accuracy       | 0.8267                    | 0.9064                    | +9.64%         |
| Precision      | 0.8298                    | 0.8819                    | +6.28%         |
| Recall         | 0.9141                    | 0.9845                    | +7.70%         |
| F1-score       | 0.8699                    | 0.9304                    | +6.95%         |

For political ideology classification:

| Metric (political) | Llama3-8b* (with context) | Llama3-70b (with context) | Percent change |
|--------------------|---------------------------|---------------------------|----------------|
| Accuracy           | 0.6923                    | 0.8504                    | +22.84%        |
| Precision          | 0.7809                    | 0.8611                    | +10.27%        |
| Recall             | 0.9141                    | 0.8504                    | +22.84%        |
| F1-score           | 0.7237                    | 0.8356                    | +15.46%        |

We get a significant improvement in performance just by using a larger model, which means that we should use as large of a model as possible.

##### How well do the results from Llama3-8b and Llama3-70b agree?

I tested as well the correlation between the smaller Llama3-8b and the larger Llama3-70b models and got the following results.

| Metric             | % agreement |
|--------------------|-------------|
| Civic/Non-civic    | 88.1%       |
| Political ideology | 71.3%       |

**Takeaway**: The smaller model does a decent job at approximating the performance of the larger model, though if we can use a larger model, we should do so.

### Takeaways from Llama testing
Here are the takeaways that I have from testing using Llama:
1. We already get pretty good performance with zero-shot models, and excellent performance for SOTA models.
2. Adding context improves performance, especially as the models get larger.
3. We get some slight improvement when using context when we use the Llama3-8b model, but we get dramatic improvements when using context with the Llama3-70b model.
Using as large a model as possible gives us free improvements on our task.
4. We likely can maximize improvements by focusing on improving the context as well as prompt engineering, rather than having to use fine-tuning.
5. We’ll have to consider class imbalance - a large portion civic content on Bluesky is left-leaning, and it is hard to find right-leaning content.

### Next steps

Now that we have something that works, the next step is to replicate the results of this pilot with a larger set of data as well as make any improvements that I observed from doing the testing.

Specifically, there are a few new experiments things that I'd like to try:
- Can we implement batching? Implementing batching would have two main speed-ups:
    - Reduce the number of requests that are sent, since we can group things together.
    - Reduce input token count (since we don't need to add the question and background preamble portions of the prompt over and over again).
- Can we improve the context?
- How does our model perform with other LLMs (e.g., Mixtral)
