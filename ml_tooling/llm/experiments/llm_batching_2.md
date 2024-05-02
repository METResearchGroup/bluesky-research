# Using LLMs to classify if social media posts are political or not (Part II).

I'm working on a project that involves gathering social media posts from [Bluesky](https://bsky.app/) and analyzing them. Part of that project requires knowing which posts are about political or social topics, and if so, what political side they support. Current ML classifiers don't work that well out of the box, so I'm trying to create our own classification scheme using LLMs. I'm trying to use LLMs in order to classify [Bluesky](https://bsky.app/) posts as either having political content or not, and if so, the political ideology, and I've found that LLMs work quite well for this task. I've used Llama3-8b and Llama3-70b via [Groq](https://groq.com/) so far, but are also open to experimenting with other open-source models as well (I have the on-prem infrastructure to host our own models, which is much cheaper at scale).

Previously, I've tried using just naive text classification and then afterwards adding context to the classifications. Now that I've shown that individual prompts work, I'd like to now work on refining this approach. We already know that it works enough for a quick pilot, but (1) it runs very slow because it runs serially, and (2) we want to improve the context that we provide to the LLM even more.

Specifically, there are a few new experiments things that I'd like to try:
- Can we change the prompt format so that the post information is given as a JSON?
- Can we implement batching?
- Can we improve the context? Can we add context about current events?
- How does our model perform with other LLMs (e.g., Mixtral)?
- Can we experiment with optimizing the prompt (e.g, with [dspy](https://github.com/stanfordnlp/dspy))?

In this notebook, I'll go over the first two points:
- Can we change the prompt format so that the post information is given as a JSON?
- Can we implement batching?

### Can we change the prompt format so that the post information is given as a JSON?

Currently, when we have posts with context, it looks something like this:

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

The formatting is not very clean and relies on adding arbitrary string chunks to our prompt. We could improve the formatting of the post information by instead structuring our code like the following:

```python
{
	"text": "<text to classify>",
	"context": {
		"content_referenced_in_post": "<Information about content referenced in the post, if any>",
		"urls_in_post": "<information about URLs in the post, if any>"
		"post_thread": "<Information about the thread that the post is a part of, if any>",
		"post_tags_and_labels": "<Information about any tags or labels in a post, if any>",
		"current_events_context": "<Any information about current events that might help the model, if any>",
		"post_author_context": "<Any information about the author that might help the model, if any>"
	}
}
```

I currently have [this](https://github.com/METResearchGroup/bluesky-research/blob/main/ml_tooling/llm/prompt_helper.py) function that does the bulk of the work in setting up the context of the prompt:

```python
def generate_context_string(
    post: dict,
    context_details_list: list[tuple],
    justify_result: bool = False,
    only_json_format: bool = False
) -> str:
    """Given a list of (context_type, context_details) tuples, generate
    the context string for the prompt."""
    post_text = post["text"]
    full_context = ""
    for context_type, context_details in context_details_list:
        full_context += f"<{context_type}>\n {context_details}\n"
    if full_context:
        full_context = f"""
The classification of a post might depend on contextual information. \
For example, the text in a post might comment on an image or on a retweeted post. \
Attend to the context where appropriate. \
Here is some context on the post that needs classification: \
'''
{full_context}
'''
Again, the text of the post that needs to be classified is:
'''
<text>
{post_text}
'''
"""  # noqa
    if justify_result:
        full_context += "\nAfter giving your label, start a new line and then justify your answer in 1 sentence."  # noqa
    else:
        full_context += "\nJustifications are not necessary."
    if only_json_format:
        full_context += "\nReturn ONLY the JSON. I will parse the string result in JSON format."
    return full_context
```

This code relies on `context_details_list`, a list of tuples where the first value describes what the type of context is and the second value is a function that generates the string for that type of context.

```python
post_context_and_funcs = [
    ("Content referenced or linked to in the post", embedded_content_context),
    ("URLs", post_linked_urls),
    ("Thread that the post is a part of", define_thread_context),
    ("Tags and labels in the post", post_tags_labels),
    ("Context about current events", additional_current_events_context),
    ("Context about the post author", post_author_context)
]


def generate_context_details_list(post: dict) -> list[tuple]:
    context_details_list = []
    for context_name, context_func in post_context_and_funcs:
        context = context_func(post)
        if context:
            context_details_list.append((context_name, context))
    return context_details_list
```

We can refactor this logic and instead create a dictionary where the keys are the context type and the values are the values that we want for that type of context.

```python
post_context_and_funcs = [
    ("content_referenced_in_post", embedded_content_context),
    ("urls_in_post", post_linked_urls)
    ("post_thread", define_thread_context),
    ("post_tags_labels", post_tags_labels),
    ("current_events_context", additional_current_events_context),
    ("post_author_context", post_author_context)
]
```

Now I just have to change the function signatures of each function in order to return a dictionary with values for that type of context. For example, I can change how my `post_author_context` is implemented in order to return a dictionary:

```python
def post_author_context(post: dict) -> dict:
    """Returns contextual information about the post's author.

    For now, we just return if the post author is a news org, but we can add to
    this later on.
    """
    author = post["author"]
    return {
        "post_author_is_reputable_news_org": author in bsky_did_to_news_org_name
    }
```

To enforce the schemas, I also created Pydantic models. This'll help us do some type checking, more conveniently update our expected schemas, and make the expected result much more apparent.

```python
from pydantic import BaseModel, Field, validator
from typing import Optional, Union


class ImagesContextModel(BaseModel):
    """Pydantic model for the images context.

    Since we don't have OCR, we can't extract text from images, but we can
    extract the alt texts of the images in the post.
    """
    image_alt_texts: Optional[str] = Field(
        default=None,
        description="The alt texts of the images in the post."
    )


class RecordContextModel(BaseModel):
    """Pydantic model for the record context, which are records that are
    referenced in a post. Records are just another name for posts, such as if
    a post links to another Bluesky post."""
    text: Optional[str] = Field(
        default=None,
        description="The text of the post."
    )
    embed_image_alt_text: Optional[ImagesContextModel] = Field(
        default=None,
        description="The alt text of the embedded image in the post."
    )


class RecordWithMediaContextModel(BaseModel):
    """Pydantic model for the record with media context.

    This is a record that has media, such as an image or video.
    """
    images_context: Optional[ImagesContextModel] = Field(
        default=None,
        description="The images context of the post."
    )
    embedded_record_context: Optional[RecordContextModel] = Field(
        default=None,
        description="The record context of the embedded post."
    )
```

That way, we can now create functions for adding context that look like this:

```python
def post_linked_urls(post: dict) -> PostLinkedUrlsContextModel:
    """Context if the post refers to any URLs in the text."""
    url_in_text_context: ContextUrlInTextModel = context_url_in_text(post)
    embed_url_context: ContextEmbedUrlModel = context_embed_url(post)

    return PostLinkedUrlsContextModel(
        url_in_text_context=url_in_text_context,
        embed_url_context=embed_url_context
    )
```

Each function takes care of a particular type of context. We can then create our overall context by chaining these functions together to look like this:

```python
post_context_and_funcs = [
    ("content_referenced_in_post", embedded_content_context),
    ("urls_in_post", post_linked_urls),
    ("post_thread", define_thread_context),
    ("post_tags_labels", post_tags_labels),
    #("current_events_context", additional_current_events_context),
    ("post_author_context", post_author_context)
]


def generate_context_details_list(post: dict) -> list[tuple]:
    """Generates a list of tuples of (context_type, context_details) for a
    post."""
    context_details_list = []
    for context_name, context_func in post_context_and_funcs:
        context = context_func(post)
        if context:
            context_details_list.append((context_name, context))
    return context_details_list


def generate_post_and_context_json(post: dict) -> dict:
    """Creates a JSON object with the post and its context.

    The JSON object has the following format:
    {
        "post": {
            "text": "The text of the post"
        },
        "context": {
            "context_type": "context_details"
        }
    }
    """
    context_details_list: list[tuple] = generate_context_details_list(post)
    context_dict = {
        # convert each Pydantic model to dict
        context_type: context_details.dict()
        for (context_type, context_details) in context_details_list
    }
    return {
        "text": post["text"],
        "context": context_dict
    }
```

This creates a JSON object that looks like this:
```python
   {
        "post": {
            "text": "The text of the post"
        },
        "context": {
            "context_type": "context_details"
        }
    }
```

If we take a post that we looked at last time:

![Example Bluesky post](assets/images/sample_post_no_context_2.png "Example Bluesky post")

We can see what the new prompt to the LLM will look like:

```plaintext
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers to whether a given post is related to politics (government, elections,
politicians, activism, etc.) or social issues (major issues that affect a large group of people, such as the economy, inequality, racism, education, immigration, human rights, the environment, etc.).
We refer to any content that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. Please classify the following text denoted in <text> as
"sociopolitical" or "not sociopolitical".

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument it presents. Your options are "democrat", "republican", or 'unclear'. You are analyzing
text that has been pre-identified as 'political' in nature. If the text is not sociopolitical, return "unclear".

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


The following JSON object contains the post and its context:
'''
    {'context': {'content_referenced_in_post': {'embedded_content_type': None, 'embedded_record_with_media_context': None, 'has_embedded_content': False},
             'post_author_context': {'post_author_is_reputable_news_org': False},
             'post_tags_labels': {'post_labels': '', 'post_tags': ''},
             'post_thread': {'thread_parent_post': {'embedded_image_alt_text': None,
                                                    'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."},
                             'thread_root_post': {'embedded_image_alt_text': None,
                                                  'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."}},
             'urls_in_post': {'embed_url_context': {'is_trustworthy_news_article': False, 'url': None}, 'url_in_text_context': {'has_trustworthy_news_links': False}}},
 'text': 'Time for Shafiq to go. Ironic her cowing to unappeasable pols to keep her job actually led to her losing it. And deserved too.'}
'''
```

It doesn't do anything to change our result:

```python
{
  "sociopolitical": "sociopolitical",
  "political_ideology": "unclear",
  "reason_sociopolitical": "The post discusses the firing of a university president, which is a sociopolitical issue.",
  "reason_political_ideology": ""
}
```

But it does clean up the prompt and make it look cleaner. We can more easily debug and see what context is added and the format is easier for the LLM to read since it's parsed many JSONs during training. Plus, this will let us start to batch posts as we can just stack these posts one on top of another.

### Batching

We want to be able to batch our LLM runs Implementing batching would give us two main speed-ups:
- Reduce the number of requests that are sent, since we can group things together.
- Reduce input token count (since we don't need to add the question and background preamble portions of the prompt over and over again).

As we're building the batching functionality, we want to keep the following requirements in mind:
- Classify posts in bulk
- Be able to error-check what the model gives us:
    - If we're classifying posts in bulk, make sure that we get as many JSONs as we expect.
    - Make sure that each JSON is properly formatted (has all the correct fields and compiles to JSON).


Ideally, we could stack our posts together in the same prompt, something like:

```python
prompt = f"""
{prompt_preamble}

<post 1>

<post 2>

<post 3>

....
"""
```

#### How many tokens are in each post?

We can use the previous post as a representative example of what the average token count would be. Only 50%-60% of posts have context (most of them are just text). Most of the others have only a little bit of context to add. Here's the JSON of that post:

```python
{
    'context': {
        'content_referenced_in_post': {'embedded_content_type': None, 'embedded_record_with_media_context': None, 'has_embedded_content': False},
        'post_author_context': {'post_author_is_reputable_news_org': False},
        'post_tags_labels': {'post_labels': '', 'post_tags': ''},
        'post_thread': {
            'thread_parent_post': {
                'embedded_image_alt_text': None,
                'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."
            },
            'thread_root_post': {
                'embedded_image_alt_text': None,
                'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."
            }
        },
        'urls_in_post': {
            'embed_url_context': {'is_trustworthy_news_article': False, 'url': None},
            'url_in_text_context': {'has_trustworthy_news_links': False}
        }
    },
    'text': 'Time for Shafiq to go. Ironic her cowing to unappeasable pols to keep her job actually led to her losing it. And deserved too.'
}
```

If we pass that post into OpenAI's [tokenizer](https://platform.openai.com/tokenizer) (most of the tokenizers give similar estimates), we see that this example is 267 tokens (or 1167 characters). We can reasonably set an upper bound for average tokens then to be something like 300 tokens per post.

#### What are the context window limits that we have to work with?

So far, I'm doing testing with [Gemini-1.0](https://blog.google/technology/ai/google-gemini-ai/) and the [Llama3](https://ai.meta.com/blog/meta-llama-3/) series (via Groq). According to their respective documentations:
- For Gemini-1.0: according to the Google [docs](https://ai.google.dev/pricing), their rate limits are:
    - 32,000 tokens per minute (TPM)
    - 15 requests per minute (RPM)
    - 1500 requests per day (RPD)
- For Llama3, I'm currently using it through Groq, and according to their [docs](https://console.groq.com/settings/limits), we have the following limits:
    - For Llama3-8b: 30,000 TPM, 30 RPM, 14,400 RPD.
    - For Llama3-70b: 7,000 TPM, 30 RPM, 14,400 RPD.


Given these rate limits, I can aim to send something between 25,000-30,000 TPM. The preamble of the prompt is about 400 tokens:

```plaintext
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers to whether a given post is related to politics (government, elections,
politicians, activism, etc.) or social issues (major issues that affect a large group of people, such as the economy, inequality, racism, education, immigration, human rights, the environment, etc.).
We refer to any content that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. Please classify the following text denoted in <text> as
"sociopolitical" or "not sociopolitical".

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument it presents. Your options are "democrat", "republican", or 'unclear'. You are analyzing
text that has been pre-identified as 'political' in nature. If the text is not sociopolitical, return "unclear".

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

If each post is about 300 tokens and the preamble is 400 tokens, then under the rate limits given, for each model, we can classify per minute, approximately (using conservative estimates):
- Gemini-1.0: 100 posts
- Llama3-8b: 95 posts
- Llama3-70b: 20 posts

In this case, for any large LLM model inference, I should use Gemini-1.0, and for a smaller model, I can use Llama3-8b.


#### Initial batching tests

I can now create a prompt that allows me to test a batch of posts in bulk:

```python
def generate_batched_post_prompt(posts: list[dict], task_name: str) -> str:
    """Create a prompt that classifies a batch of posts."""
    task_prompt = task_name_to_task_prompt_map[task_name]
    task_prompt += """
You will receive a batch of posts to classify. The batch of posts is in a JSON
with the following fields: "posts": a list of posts, and "expected_number_of_posts":
the number of posts in the batch.

Each post will be its own JSON \
object including the text to classify (in the "text" field) and the context \
in which the post was made (in the "context" field). Return a list of JSONs \
for each post, in the format specified before. The length of the list of \
JSONs must match the value in the "expected_number_of_posts" field.

Return a JSON with the following format:
{
    "results": <list of JSONs, one for each post>,
    "count": <number of posts classified. Must match `expected_number_of_posts`>
}
"""
    post_contexts = []
    for post in posts:
        post_contexts.append(generate_post_and_context_json(post))
    batched_context = {
        "posts": post_contexts,
        "expected_number_of_posts": len(posts)
    }
    batched_context_str = pformat(batched_context, width=200)
    full_prompt = f"""
{task_prompt}

{batched_context_str}
    """
    print(f"Length of batched_context_str: {len(batched_context_str)}")
    print(f"Length of full prompt: {len(full_prompt)}")
    return full_prompt
```

Let's see how well this works.

I've previously created a Streamlit [app](https://github.com/METResearchGroup/bluesky-research/blob/main/demos/post_to_llm_labeling/app.py) to demo LLM classifications of Bluesky posts. We can update that to accept a comma-separated list of posts, in order to see how it looks when we want to classify posts in bulk.

```python
bulk_bluesky_posts = st.text_area(
    "Enter multiple Bluesky links/texts separated by commas"
)

if bulk_bluesky_posts:
    bulk_classify_posts = st.checkbox(
        "Classify all the posts in bulk?"
    )

# run the labeling and print the result
if st.button('Submit'):

    # manage bulk posts
    if bulk_classify_posts:
        bsky_post_links: list[str] = bulk_bluesky_posts.split(",")
        posts: list[dict] = [
            convert_post_link_to_post(
                post_link, include_author_info=True
            ) for post_link in bsky_post_links
        ]
        task_name: str = option_to_task_name_map[option]
        prompt: str = generate_batched_post_prompt(
            posts=posts, task_name=task_name
        )
        wrapped_prompt = wrap_text(prompt, width=200)
        st.text(wrapped_prompt)
        st.subheader("Result from the LLM")
        llm_result = run_query(prompt=prompt, model_name=model_name)
        st.text(wrap_text(llm_result, width=120))
        print(llm_result)
```

If we take a look at the following:
![Streamlit input for bulk inference](assets/images/streamlit_bulk_inference_input.png "Streamlit input for bulk inference")

we get the following prompt to the LLM:

```plaintext
Pretend that you are a classifier that predicts whether a post has sociopolitical content or not. Sociopolitical refers to whether a given post is related to politics (government, elections,
politicians, activism, etc.) or social issues (major issues that affect a large group of people, such as the economy, inequality, racism, education, immigration, human rights, the environment, etc.).
We refer to any content that is classified as being either of these two categories as "sociopolitical"; otherwise they are not sociopolitical. Please classify the following text as "sociopolitical" or
"not sociopolitical".

Then, if the post is sociopolitical, classify the text based on the political lean of the opinion or argument it presents. Your options are "democrat", "republican", or 'unclear'. You are analyzing
text that has been pre-identified as 'political' in nature. If the text is not sociopolitical, return "unclear".

Think through your response step by step.

Return in a JSON format in the following way:
{
    "sociopolitical": <two values, 'sociopolitical' or 'not sociopolitical'>,
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'>,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical. If none, return an empty string, "">,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology. If none, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.

You will receive a batch of posts to classify. The batch of posts is in a JSON
with the following fields: "posts": a list of posts, and "expected_number_of_posts":
the number of posts in the batch.

Each post will be its own JSON object including the text to classify (in the "text" field) and the context in which the post was made (in the "context" field). Return a list of JSONs for each post, in
the format specified before. The length of the list of JSONs must match the value in the "expected_number_of_posts" field.

Return a JSON with the following format:
{
    "results": <list of JSONs, one for each post>,
    "count": <number of posts classified. Must match `expected_number_of_posts`>
}


{'expected_number_of_posts': 2,
 'posts': [{'context': {'content_referenced_in_post': {'embedded_content_type': None, 'embedded_record_with_media_context': None, 'has_embedded_content': False},
                        'post_author_context': {'post_author_is_reputable_news_org': False},
                        'post_tags_labels': {'post_labels': '', 'post_tags': ''},
                        'post_thread': {'thread_parent_post': {'embedded_image_alt_text': None,
                                                               'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."},
                                        'thread_root_post': {'embedded_image_alt_text': None,
                                                             'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."}},
                        'urls_in_post': {'embed_url_context': {'is_trustworthy_news_article': False, 'url': None}, 'url_in_text_context': {'has_trustworthy_news_links': False}}},
            'text': 'Time for Shafiq to go. Ironic her cowing to unappeasable pols to keep her job actually led to her losing it. And deserved too.'},
           {'context': {'content_referenced_in_post': {'embedded_content_type': None, 'embedded_record_with_media_context': None, 'has_embedded_content': False},
                        'post_author_context': {'post_author_is_reputable_news_org': False},
                        'post_tags_labels': {'post_labels': '', 'post_tags': ''},
                        'post_thread': {'thread_parent_post': {'embedded_image_alt_text': None, 'text': None}, 'thread_root_post': {'embedded_image_alt_text': None, 'text': None}},
                        'urls_in_post': {'embed_url_context': {'is_trustworthy_news_article': False, 'url': None}, 'url_in_text_context': {'has_trustworthy_news_links': False}}},
            'text': 'just got an email from my alma mater saying it had been compelled to deploy police to defend the Community from violent outside agitators who had begun to protest on the quad; '
                    'the first video in my feed is cops in full gear arresting the chair of the philosophy department'}]}
```

We get the following result:

```python
{
    "results": [
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post discusses the firing of a university president, which is a political
issue.",
            "reason_political_ideology": "The post criticizes the president for \"cowing to unappeasable pols\", which
suggests a left-leaning perspective."
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post discusses the deployment of police to defend a university from
protesters, which is a sociopolitical issue.",
            "reason_political_ideology": "The post does not provide enough information to determine the political
ideology of the author."
        }
    ],
    "count": 2
}
```

### Load testing the inference time

Looks like it works, at least for a batch of 2 posts. Let's see if we can do this for more posts. Let me try it for 10 posts. Let's also take a look at how long it takes as we increase the number of posts. For Gemini, our token limit restricts us to <100 posts per minute. I'll try varying numbers of posts. I'll take a random subset of Bluesky posts from the [Week Peak Feed](https://bsky.app/profile/did:plc:tenurhgjptubkk5zf5qhi3og/feed/catch-up-weekly), the most popular posts from the past week on Bluesky.

| Number of posts | Token count | How long it takes (in seconds) | Any errors or problems that are encountered |
|-----------------|-------------|--------------------------------|---------------------------------------------|
| 2               |     1173    |             8                  |                    N/A                      |
| 5               |     1845    |            15                  |                    N/A                      |
| 10              |     2889    |            18                  |                    N/A                      |
| 20              |     5360    |            27                  |                    N/A                      |

I had planned on testing with more samples, but even with a small batch size I already see some flaws:

For n=2, I used the following links:
- https://bsky.app/profile/bencollins.bsky.social/post/3kqyj7u2zqh27
- https://bsky.app/profile/k8lister.bsky.social/post/3krcbkos4mk2j

The LLM returned the following response:
```plaintext
{
    "results": [
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The text discusses a social issue (gender inequality).",
            "reason_political_ideology": "The text does not express a clear political ideology."
        }
    ],
    "count": 2
}
```

But, I use those same links with n=5, plus three more links:
- https://bsky.app/profile/bencollins.bsky.social/post/3kqyj7u2zqh27
- https://bsky.app/profile/k8lister.bsky.social/post/3krcbkos4mk2j
- https://bsky.app/profile/bsky.app/post/3kqy4mjw5eo2t
- https://bsky.app/profile/shoshana.bsky.social/post/3kr4yymcvp62u
- https://bsky.app/profile/bencollins.bsky.social/post/3kqykn643ps2l

and I get the following:
```plaintext
{
    "results": [
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        }
    ],
    "count": 5
}
```

It's strange that the scoring on the second post changed when a larger batch size was used. To be fair, this one seems to be a more on-the-border case.

For n=10 links:
- https://bsky.app/profile/bencollins.bsky.social/post/3kqyj7u2zqh27
- https://bsky.app/profile/k8lister.bsky.social/post/3krcbkos4mk2j
- https://bsky.app/profile/bsky.app/post/3kqy4mjw5eo2t
- https://bsky.app/profile/shoshana.bsky.social/post/3kr4yymcvp62u
- https://bsky.app/profile/bencollins.bsky.social/post/3kqykn643ps2l
- https://bsky.app/profile/book-historia.bsky.social/post/3krbvbufoil27
- https://bsky.app/profile/existentialcomics.bsky.social/post/3krexyc57oc2g
- https://bsky.app/profile/juicysteak117.gay/post/3krfkyfh3pc24
- https://bsky.app/profile/criminalerin.bsky.social/post/3kqxlnytdh52n
- https://bsky.app/profile/patblanchfield.bsky.social/post/3kqxzvonqis2u


We get the following results:

```plaintext
{
    "results": [
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The text discusses the history of college protests and the role of the New York
Times in shaping public opinion on American wars.",
            "reason_political_ideology": "The text criticizes the New York Times, which is often seen as a liberal
publication, for its support of American wars."
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The text does not discuss politics or social issues.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The text discusses climate change and the role of police in suppressing
protests.",
            "reason_political_ideology": "The text expresses concern about climate change and police violence, which are
both issues that are often associated with the Democratic Party."
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The text discusses police violence and the arrest of a philosophy department
chair.",
            "reason_political_ideology": "The text expresses concern about police violence, which is an issue that is
often associated with the Democratic Party."
        }
    ],
    "count": 10
}
```

The classifications on the last 5 appears to be correct, and it does seem like at first blush that we get good results. 

For n=20 links:
- https://bsky.app/profile/bencollins.bsky.social/post/3kqyj7u2zqh27
- https://bsky.app/profile/k8lister.bsky.social/post/3krcbkos4mk2j
- https://bsky.app/profile/bsky.app/post/3kqy4mjw5eo2t
- https://bsky.app/profile/shoshana.bsky.social/post/3kr4yymcvp62u
- https://bsky.app/profile/bencollins.bsky.social/post/3kqykn643ps2l
- https://bsky.app/profile/book-historia.bsky.social/post/3krbvbufoil27
- https://bsky.app/profile/existentialcomics.bsky.social/post/3krexyc57oc2g
- https://bsky.app/profile/juicysteak117.gay/post/3krfkyfh3pc24
- https://bsky.app/profile/criminalerin.bsky.social/post/3kqxlnytdh52n
- https://bsky.app/profile/patblanchfield.bsky.social/post/3kqxzvonqis2u
- https://bsky.app/profile/skipbidder.bsky.social/post/3krbzi5ppv32f
- https://bsky.app/profile/kjhealy.bsky.social/post/3kqw56xals22q
- https://bsky.app/profile/mosheroperandi.bsky.social/post/3kqx2t4u3gl2c
- https://bsky.app/profile/profmusgrave.bsky.social/post/3kqxt2gcv3h2h
- https://bsky.app/profile/audrelawdamercy.bsky.social/post/3krfez3ctas2h
- https://bsky.app/profile/tylerjameshill.bsky.social/post/3kr37elsa3k2j
- https://bsky.app/profile/rincewind.run/post/3kqxtmpj5ds2h
- https://bsky.app/profile/beagle.bsky.social/post/3kqw44stwkc2m
- https://bsky.app/profile/sjjphd.bsky.social/post/3krdvhoyqxs2e
- https://bsky.app/profile/alexanderchee.bsky.social/post/3kqzuonxwns2z

We get the following results:
```plaintext
{
    "results": [
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post discusses the sale of The Onion, a satirical news website.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post discusses the author's experience with angry men who disagree with her
opinion piece on women's safety.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post announces the addition of GIFs to the Bluesky platform.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a caption for an image of a toy squirt gun.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a response to another post about buying The Onion.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a caption for an image of a book with a child's drawing on the
cover.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post criticizes the New York Times and Harvard-educated opinion writers for
being wrong about American wars.",
            "reason_political_ideology": "The post aligns with the Democratic Party's general stance of criticizing the
military-industrial complex and supporting anti-war efforts."
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a caption for an image of a truck.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post discusses the earlier start of cop riot season and connects it to climate
change.",
            "reason_political_ideology": "The post aligns with the Democratic Party's general stance on climate change
and police brutality."
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post discusses the arrest of a philosophy department chair by police in riot
gear.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a caption for an image of teaspoons in plastic bags.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post discusses the use of a sign to keep cops off school property during a
protest.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a question about noise outside the author's window.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post discusses the Supreme Court's decision not to overturn a death row
sentence and compares it to Trump's request to be declared a king.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post criticizes the police response to student protests and calls for
accountability.",
            "reason_political_ideology": "The post aligns with the Democratic Party's general stance on police brutality
and support for student activism."
        },
        {
            "sociopolitical": "not sociopolitical",
            "political_ideology": "",
            "reason_sociopolitical": "The post is a caption for an image of a sheep.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post criticizes the idea that the President is a king and argues that it is
insulting to the country's founding principles.",
            "reason_political_ideology": "The post aligns with the Democratic Party's general stance on the separation
of powers and the rule of law."
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post announces a strike by UT Austin faculty in protest of the police presence
on campus.",
            "reason_political_ideology": "The post aligns with the Democratic Party's general stance on police brutality
and support for student activism."
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "unclear",
            "reason_sociopolitical": "The post quotes Frederick Douglass on the importance of struggle and agitation.",
            "reason_political_ideology": ""
        },
        {
            "sociopolitical": "sociopolitical",
            "political_ideology": "democrat",
            "reason_sociopolitical": "The post criticizes the police response to student protests and compares it to the
police inaction during the Uvalde school shooting.",
            "reason_political_ideology": "The post aligns with the Democratic Party's general stance on police brutality
and support for gun control."
        }
    ],
    "count": 20
}
```

It looks like with n=20, we still get good results. I suspect that this will get worse though as we use a smaller model in production. It seems like I should aim for using as small a batch size as possible, and then just maximizing the number of requests. We have a 15 RPM limit for the models, so we could reasonably create smaller batch sizes and just send more requests. Doing so would let us more reasonably control the quality of posts, reduce the chance of hallucination, and narrow the blast radius of any errors in the model's inference.


## Takeaways

I got the batching to work. I re-worked the prompting to allow for multiple posts to be classified at the same time. Initial testing looks pretty good, though I'll need to experiment more to see how accurate it does in the long term plus tease out the tendency for model hallucination, especially as I move to smaller models.


