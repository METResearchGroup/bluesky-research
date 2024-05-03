# Using LLMs to classify if social media posts are political or not (Part III).

I'm working on a project that involves gathering social media posts from [Bluesky](https://bsky.app/) and analyzing them. Part of that project requires knowing which posts are about political or social topics, and if so, what political side they support. Current ML classifiers don't work that well out of the box, so I'm trying to create our own classification scheme using LLMs. I'm trying to use LLMs in order to classify [Bluesky](https://bsky.app/) posts as either having political content or not, and if so, the political ideology, and I've found that LLMs work quite well for this task. I've used Llama3-8b and Llama3-70b via [Groq](https://groq.com/) so far, but are also open to experimenting with other open-source models as well (I have the on-prem infrastructure to host our own models, which is much cheaper at scale).

Previously, I've tried using just naive text classification and then afterwards adding context to the classifications. Now that I've shown that individual prompts work, I've also worked on how to include batching and adding some (simple) scaling for the prompts. I'd like to improve the approach even more.

Specifically, there are a few new experiments things that I'd like to try:
- Can we improve the context? Can we add context about current events?
- How does our model perform with other LLMs (e.g., Mixtral)?
- Can we experiment with optimizing the prompt (e.g, with [dspy](https://github.com/stanfordnlp/dspy))?

In this notebook, I'll go over the first point:
- Can we improve the context? Can we add context about current events?

### Adding context about current events

LLMs have a hard knowledge cutoff date. Although they'll generally encode world knowledge, LLMs won't have specific knowledge of what's currently in the news. If we want to classify political content, we'll have to be able to provide the LLM with knowledge of coverage about certain topics, else it won't be able to make informed classifications.

#### Example post that could use current events knowledge

Here is an example post whose classification would change if the LLM knew what was happening in the news:

![Example Bluesky post](assets/images/sample_post_3.png "Example Bluesky post")

If we use the following prompt, which has the original text of the post plus some additional context:

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
    "political_ideology": <three values, 'democrat', 'republican', 'unclear'. If the post is not sociopolitical, return an empty string, "">,
    "reason_sociopolitical": <optional, a 1 sentence reason for why the text is sociopolitical or not.>,
    "reason_political_ideology": <optional, a 1 sentence reason for why the text has the given political ideology or is unclear. If the post is not sociopolitical, return an empty string, "">
}

All of the fields in the JSON must be present for the response to be valid, and the answer must be returned in JSON format.


Here is the post text that needs to be classified:
<text>
Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence.


The following JSON object contains the post and its context:

{'context': {'content_referenced_in_post': {'embedded_content_type': None,
                                            'embedded_record_with_media_context': {'description': 'Massive faculty walkout at @Columbia opposing the university’s decision to call in NYPD on '
                                                                                                  'Palestine solidarity protests: https://t.co/DcCSxObtx9',
                                                                                   'title': 'Bassam Khawaja on X: "Massive faculty walkout at @Columbia opposing the university’s decision to call in '
                                                                                            'NYPD on Palestine solidarity protests: https://t.co/DcCSxObtx9" / X'},
                                            'has_embedded_content': True},
             'post_author_context': {'post_author_is_reputable_news_org': False},
             'post_tags_labels': {'post_labels': '', 'post_tags': ''},
             'post_thread': {'thread_parent_post': {'embedded_image_alt_text': None, 'text': None}, 'thread_root_post': {'embedded_image_alt_text': None, 'text': None}},
             'urls_in_post': {'embed_url_context': {'is_trustworthy_news_article': False, 'url': 'https://twitter.com/Bassam_Khawaja/status/1782471549007606164'},
                              'url_in_text_context': {'has_trustworthy_news_links': False}}},
 'text': "Faculty walkout at Columbia. I think it's safe to say President Shafiq & other leaders have lost their confidence."}


Justifications are not necessary.
```

We get the following label:

```plaintext
{
    "sociopolitical": "sociopolitical",
    "political_ideology": "unclear",
    "reason_sociopolitical": "The post discusses a faculty walkout at Columbia University, which is a social issue.",
    "reason_political_ideology": ""
}
```

Our context is not helpful because the LLM doesn't have knowledge of the Columbia protests. Ideally, 

If we change the prompt to something like this:

```plaintext
<same prompt as before>

In the news right now, this is what each political side has said about the Columbia protests. Use this as context for classifying political ideology:
- Democrat: The students and faculty have lost confidence in Columbia for calling the NYPD on the Palestine solidarity protests. People have lost confidence in the leaders of Columbia.
- Republican: The students are occupying these schools and are antisemitic. The administration was in the right to protect the campus by using the NYPD.
```

We get the following result:
```plaintext
{
    "sociopolitical": "sociopolitical",
    "political_ideology": "democrat",
    "reason_sociopolitical": "The text discusses a faculty walkout at Columbia, relating to decisions made by university leadership, which is a matter of public concern and activism.",
    "reason_political_ideology": "The text implies a loss of confidence in leadership due to decisions affecting social activism, aligning with the Democratic perspective supporting the protests."
}
```

This is what having context about current events can give to us. It can give the model the key information needed to classify posts that talk about current events.

**Caveat**: We won't be able to catch every possible nuance or every single trend or piece of slang. The references have to be pretty direct and clear for us to easily detect it. On the other hand, it is worth a shot to implement at least a simplified means of getting knowledge about current events to the LLM and having it use that as context for its decisions.

### Setting up access to the news

We can use the [NewsAPI](https://newsapi.org/) service to grab the latest current events. This is a free API that gives us access to the latest current events, as covered by a variety of news outlets.

We can set up access to the NewsAPI service through their [Python client](https://github.com/mattlisiv/newsapi-python):

```python
from newsapi import NewsApiClient

newsapi_client = NewsApiClient(api_key="<NEWSAPI_API_KEY>")
```

We can then get the list of US news domains that are available:

```python
# https://newsapi.org/docs/endpoints/sources
sources: list[dict] = newsapi_client.get_sources(country="us")
urls: list[dict] = [source["url"] for source in sources["sources"]]

def parse_domain_from_url(url: str) -> str:
    """Given a URL, parse the domain.

    Example:
    >>> parse_domain_from_url("https://www.nytimes.com")
    "nytimes.com"
    >>> parse_domain_from_url("https://www.foxnews.com")
    "foxnews.com"
    >>> parse_domain_from_url("http://www.foxnews.com")
    "foxnews.com"
    >>> parse_domain_from_url("www.foxnews.com")
    "foxnews.com"
    """
    link = url
    if "https://" in url:
        link = url.replace("https://", "")
    elif "http://" in url:
        link = url.replace("http://", "")
    if "www." in link:
        link = link.replace("www.", "")
    return link


def parse_url(url: str) -> str:
    """Given the URL, parse it. Grab domain plus do postprocessing.

    Get the domain, plus remove anything like trailing subpages.
    """
    parsed_url = parse_domain_from_url(url)
    parsed_url = parsed_url.split("/")[0]
    return parsed_url


news_domains = [parse_url(url) for url in urls]
```

By doing this, we get the following list of domains:

```plaintext
['abcnews.go.com', 'aljazeera.com', 'arstechnica.com', 'apnews.com', 'axios.com', 'bleacherreport.com', 'bloomberg.com', 'breitbart.com', 'businessinsider.com', 'buzzfeed.com', 'cbsnews.com', 'us.cnn.com', 'cnnespanol.cnn.com', 'ccn.com', 'engadget.com','ew.com', 'espn.com',
 'espncricinfo.com', 'fortune.com', 'foxnews.com','foxsports.com', 'news.google.com', 'news.ycombinator.com', 'ign.com', 'mashable.com', 'medicalnewstoday.com', 'msnbc.com', 'mtv.com', 'news.nationalgeographic.com', 'nationalreview.com', 'nbcnews.com', 'newscientist.com', 'newsweek.com', 'nymag.com', 'nextbigfuture.com', 'nfl.com', 'nhl.com', 'politico.com', 'polygon.com', 'recode.net', 'reddit.com', 'reuters.com', 'techcrunch.com', 'techradar.com', 'theamericanconservative.com', 'thehill.com', 'huffingtonpost.com', 'thenextweb.com', 'theverge.com',
 'wsj.com', 'washingtonpost.com', 'washingtontimes.com', 'time.com', 'usatoday.com', 'news.vice.com','wired.com']
```

We definitely won't need access to all of these, but we likely want a set that's representative of news coverage across the political spectrum. Luckily, we can use [AllSides](https://allsides.com/) for that information. AllSides collects news coverage from across the political spectrum and presents the varying coverage of a certain topic across the aisle. They provide a [Media Bias Chart](https://www.allsides.com/media-bias/media-bias-chart) that shows, in their coverage, the relative media bias of different news outlets. They determine their media bias by a combination of crowdsourced polls across their reader audience as well as expert opinions.

Conveniently, AllSides also provides specific information on their bias scoring for individual news orgs, such as [Bloomberg](https://www.allsides.com/news-source/bloomberg-media-bias).

![AllSides Media Bias Chart](assets/images/allsides_media_bias_chart.png "AllSides Media Bias Chart")

Using this chart and cross-referencing the AllSides website, we can create a mapping of what political leaning each news outlet has:

```plaintext
political_party_to_news_outlet_domains_map = {
    "democrat": [
        "abcnews.go.com", "msnbc.com", "apnews.com", "axios.com",
        "bloomberg.com", "cbsnews.com", "us.cnn.com", "politico.com",
        "washingtonpost.com", "time.com", "usatoday.com", "news.vice.com",
        "aljazeera.com", "businessinsider.com", "news.google.com"
    ],
    "moderate": [
        "wsj.com", "newsweek.com", "reuters.com", "fortune.com", "thehill.com"
    ],
    "conservative": [
        "theamericanconservative.com", "nationalreview.com",
        "washingtontimes.com", "foxnews.com", "breitbart.com"
    ]
}
```

### Loading news from each source

For a given source, we can use the Python client to get all the top headlines using [this](https://github.com/mattlisiv/newsapi-python/blob/master/newsapi/newsapi_client.py#L34) method.

To do this, we need to use the IDs of the different news outlets, not just the domains, so we need to grab that:

```plaintext
political_party_to_news_outlets = {
    'conservative': [
        {'domain': 'theamericanconservative.com', 'id': 'the-american-conservative'},
        {'domain': 'nationalreview.com', 'id': 'national-review'},
        {'domain': 'washingtontimes.com', 'id': 'the-washington-times'},
        {'domain': 'foxnews.com', 'id': 'fox-news'},
        {'domain': 'breitbart.com', 'id': 'breitbart-news'}
    ],
    'democrat': [
        {'domain': 'abcnews.go.com', 'id': 'abc-news'},
        {'domain': 'msnbc.com', 'id': 'msnbc'},
        {'domain': 'apnews.com', 'id': 'associated-press'},
        {'domain': 'axios.com', 'id': 'axios'},
        {'domain': 'bloomberg.com', 'id': 'bloomberg'},
        {'domain': 'cbsnews.com', 'id': 'cbs-news'},
        {'domain': 'us.cnn.com', 'id': 'cnn'},
        {'domain': 'politico.com', 'id': 'politico'},
        {'domain': 'washingtonpost.com', 'id': 'the-washington-post'},
        {'domain': 'time.com', 'id': 'time'},
        {'domain': 'usatoday.com', 'id': 'usa-today'},
        {'domain': 'news.vice.com', 'id': 'vice-news'},
        {'domain': 'aljazeera.com', 'id': 'al-jazeera-english'},
        {'domain': 'businessinsider.com', 'id': 'business-insider'},
        {'domain': 'news.google.com', 'id': 'google-news'}
    ],
    'moderate': [
        {'domain': 'wsj.com', 'id': 'the-wall-street-journal'},
        {'domain': 'newsweek.com', 'id': 'newsweek'},
        {'domain': 'reuters.com', 'id': 'reuters'},
        {'domain': 'fortune.com', 'id': 'fortune'},
        {'domain': 'thehill.com', 'id': 'the-hill'}
    ]
}
```

Let's see what we get if we get the top headlines for one of these news outlets.

```python
headlines = newsapi_client.get_top_headlines(sources="abc-news")
headlines["articles"][0:3]
```

```plaintext
[
    {
        'source': {'id': 'abc-news', 'name': 'ABC News'},
        'author': 'TOM MURPHY AP health writer',
        'title': 'CVS Health chops 2024 forecast as cost struggles with Medicare Advantage persist',
        'description': 'CVS Health missed first-quarter expectations and chopped its 2024 outlook more than a dollar below Wall Street’s forecast',
        'url': 'https://abcnews.go.com/Business/wireStory/cvs-health-chops-2024-forecast-cost-struggles-medicare-109821867',
        'urlToImage': 'https://i.abcnewsfe.com/a/621b804d-d800-4c41-8e67-e0b453912b53/wirestory_1dcdd232c1648f86a2cfc87dc09ab72f_16x9.jpg?w=1600',
        'publishedAt': '2024-05-01T12:15:40Z',
        'content': 'CVS Health missed first-quarter expectations and chopped its 2024 outlook more than a dollar below Wall Streets forecast.\r\nShares of the health care giant plunged Wednesday morning after the company … [+1291 chars]'
    },
    {
        'source': {'id': 'abc-news', 'name': 'ABC News'},
        'author': 'The Associated Press',
        'title': '17-year-old boy charged with attempted murder after assaulting 3 at school in England',
        'description': 'Police say a 17-year-old boy has been arrested on suspicion of attempted murder after three people were assaulted with a sharp object at a secondary school in Sheffield in northern England',
        'url': 'https://abcnews.go.com/International/wireStory/17-year-boy-charged-attempted-murder-after-assaulting-109818807',
        'urlToImage': 'https://i.abcnewsfe.com/a/d920dad6-cb32-4731-83a5-99fc4bd2f43f/wirestory_79206eaebef0d6b379f8053ecc062584_16x9.jpg?w=1600',
        'publishedAt': '2024-05-01T10:21:40Z',
        'content': 'LONDON -- A 17-year-old boy has been arrested on suspicion of attempted murder after three people were assaulted with a sharp object at a secondary school in northern England, South Yorkshire Police … [+303 chars]'
    },
    {
        'source': {'id': 'abc-news', 'name': 'ABC News'},
        'author': 'Aaron Katersky, Meredith Deliso',
        'title': 'Harvey Weinstein scheduled to appear in court after sex crimes conviction overturned',
        'description': 'His attorney said they are prepared to go to trial again, "if it comes to that."',
        'url': 'https://abcnews.go.com/US/harvey-weinstein-manhattan-court-appearance/story?id=109795381',
        'urlToImage': 'https://i.abcnewsfe.com/a/7084e61b-6abb-4f8e-899f-0c7364e53cdb/harvey-weinstein-gty-jef-240430_1714498214340_hpMain_16x9.jpg?w=1600',
        'publishedAt': '2024-05-01T09:15:22Z',
        'content': "Harvey Weinstein is scheduled to appear in court in Manhattan on Wednesday for the first time since New York's highest court overturned his sex crimes conviction.\r\nHe is scheduled to appear in Manhat… [+3133 chars]"
    }
]
```

```python
headlines["articles"][1]["content"]
```

```plaintext
'LONDON -- A 17-year-old boy has been arrested on suspicion of attempted murder after three people were assaulted with a sharp object at a secondary school in northern England, South Yorkshire Police … [+303 chars]'
```



### Getting the latest news across outlets

Now we can access the news articles from a given news provider. The limitation behind this approach is that we can't get the full text of a news article, all we get is a preview of the content. But we do get the headline and the preview text. We can generally, as people, tell what stance a news outlet has towards a topic based on what they choose to focus on, and we can provide that as context to our model.

```python
def get_latest_top_headlines_for_source(source_id: str) -> list[dict]:
    """Get the latest top headlines from a news source."""
    articles = newsapi_client.get_top_headlines(sources=source_id)["articles"]
    return [
        {
            "title": article["title"],
            "description": article["description"],
            "url": article["url"],
            "content": article["content"],
        }
        for article in articles
    ]


def get_latest_top_headlines_for_political_party(
    political_party: str
) -> list[dict]:
    """Get the latest top headlines from a news source."""
    news_outlets = political_party_to_news_outlets[political_party]
    return [
        {
            "source": news_outlet["id"],
            "articles": get_latest_top_headlines_for_source(news_outlet["id"]),
        }
        for news_outlet in news_outlets
    ]


def get_latest_articles() -> dict:
    """Get the latest top headlines from a news source."""
    return {
        "conservative": get_latest_top_headlines_for_political_party("conservative"), # noqa
        "moderate": get_latest_top_headlines_for_political_party("moderate"),
        "democrat": get_latest_top_headlines_for_political_party("democrat"),
    }

latest_articles = get_latest_articles()
conservative_headlines = latest_articles['conservative']
pprint(conservative_headlines[1][0:5])
```

```plaintext
{
    'articles': [
        {
            'content': None,
            'description': 'Performers allegedly wore sexually suggestive '
                              'clothing and prosthetic female breasts and '
                              'genitalia.',
            'title': 'DeSantis Admin Revokes Liquor License of Orlando '
                    'Venue That Hosted Sexual Drag Show for Children',
            'url': 'https://www.nationalreview.com/news/desantis-admin-revokes-liquor-license-of-orlando-venue-that-hosted-sexual-drag-show-for-children/'
        },
        {
            'content': None,
            'description': 'Isabel Vaughan-Spruce has nevertheless decided to pursue a verdict in court to clear her name.',
            'title': 'Charges Dropped against British Woman Arrested for Praying outside Abortion Clinic',
            'url': 'https://www.nationalreview.com/news/charges-dropped-against-british-woman-arrested-for-praying-outside-abortion-clinic/'
        },
        {
            'content': None,
            'description': 'The former representatives who wrote the HEROES Act told SCOTUS it’s being ‘misused and 'distorted’ to advance the administration’s student-loan plan.',
            'title': 'Authors of the Law Biden Used to Wipe Out Student Debt Declare He Doesn’t Have the Authority',
            'url': 'https://www.nationalreview.com/2023/02/authors-of-the-law-biden-used-to-wipe-out-student-debt-declare-he-doesnt-have-the-authority/'
        },
        {
            'content': None,
            'description': 'Universities have been conducting an 'unprecedented experiment in historical amnesia on American students.',
            'title': 'Why We Need Western Civ',
            'url': 'https://www.nationalreview.com/2023/02/why-we-need-western-civ/'
        },
        {
            'content': None,
            'description': 'Chinese officials confirmed the balloon is theirs, but claimed it’s a civilian airship used for research that was blown off course.',
            'title': 'Pentagon Press Secretary Disputes China’s Claim That Balloon Is Civilian Airship',
            'url': 'https://www.nationalreview.com/news/mccarthy-demands-gang-of-eight-briefing-on-chinese-spy-balloon/'
        },
        {
            'content': None,
            'description': 'The contradictions of the Colorado Court of Appeals opinion premises regarding Jack Phillips are very clear.',
            'title': 'Speech for Me, Not for Thee',
            'url': 'https://www.nationalreview.com/corner/speech-for-me-not-for-thee/'
        },
        {
            'content': None,
            'description': 'Joe Biden remains Democrats’ best choice for 2024, and that’s not really a good thing.',
            'title': 'Democrats Are Stuck with Biden',
            'url': 'https://www.nationalreview.com/2023/02/democrats-are-stuck-with-biden/'
        },
        {
            'content': None,
            'description': 'Eunice Dwumfour reportedly had just stopped by her house and was about to leave again in her car when the assailant confronted her and shot her.',
            'title': 'GOP New Jersey Councilwoman Shot to Death outside Her Home',
            'url': 'https://www.nationalreview.com/news/gop-new-jersey-councilwoman-shot-to-death-outside-her-home/'
        },
        {
            'content': None,
            'description': '‘I didn’t feel right being in either category. . . . The only thing that felt right to me would be to abstain from nomination consideration’',
            'title': 'Nonbinary Broadway Star Rejects Tony Award Consideration over Gendered Categories',
            'url': 'https://www.nationalreview.com/news/nonbinary-broadway-star-rejects-tony-award-consideration-over-gendered-categories/'
        },
        {
            'content': None,
            'description': 'The AG offices of Missouri, Texas, and Arizona have led the fight against Biden overreach, but insiders tell NR they’re not in a position to keep it up.',
            'title': 'Legal Resistance to Biden Administration in Doubt as Powerhouse AG Offices Stumble',
            'url': 'https://www.nationalreview.com/news/legal-resistance-to-biden-administration-in-doubt-as-powerhouse-ag-offices-stumble/?utm_source=recirc-%5BSCREENSIZE%5D&#038;utm_medium=homepage&#038;utm_campaign=hero&#038;utm_content=related&#038;utm_term=first'
        }
    ],
    'source': 'national-review'
}
```

### Store articles in a database

Let's first store our articles in a database. I'm using SQLite for my database and [peewee](https://pypi.org/project/peewee/) as a simple ORM.

```python
current_file_directory = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_NAME = "news.db"
SQLITE_DB_PATH = os.path.join(current_file_directory, SQLITE_DB_NAME)

db = peewee.SqliteDatabase(SQLITE_DB_PATH)
db_version = 2

conn = sqlite3.connect(SQLITE_DB_PATH)
cursor = conn.cursor()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class NewsOutlet(BaseModel):
    """News outlet model."""
    outlet_id = peewee.CharField(primary_key=True)
    domain = peewee.CharField()
    political_party = peewee.CharField()


class NewsArticle(BaseModel):
    """News article model."""
    url = peewee.CharField(primary_key=True)
    title = peewee.CharField()
    content = peewee.TextField()
    description = peewee.TextField()
    publishedAt = peewee.CharField()
    news_outlet_source_id = peewee.ForeignKeyField(
        NewsOutlet, field='outlet_id', backref='articles'
    )


if db.is_closed():
    db.connect()
    db.create_tables([NewsOutlet, NewsArticle])


def create_initial_tables() -> None:
    with db.atomic():
        db.create_tables([NewsOutlet, NewsArticle])


def insert_news_outlet(news_outlet: dict) -> None:
    with db.atomic():
        NewsOutlet.create(**news_outlet)


def insert_news_article(news_article: dict) -> None:
    with db.atomic():
        NewsArticle.create(**news_article)
```

We can also create some Pydantic models just to make sure that our schemas are as we expect and so we can add validation checks if needed.

```python
from pydantic import BaseModel, Field

class NewsOutletModel(BaseModel):
    """Pydantic model for the news outlet."""
    outlet_id: str = Field(
        description="The ID of the news outlet."
    )
    domain: str = Field(
        description="The domain of the news outlet."
    )
    political_party: str = Field(
        description="The political party of the news outlet."
    )
    synctimestamp = Field(
        description="The timestamp of the sync."
    )


class NewsArticleModel(BaseModel):
    """Pydantic model for the news article."""
    url: str = Field(
        description="The URL of the news article."
    )
    title: str = Field(
        description="The title of the news article."
    )
    content: str = Field(
        description="The content of the news article."
    )
    description: str = Field(
        description="The description of the news article."
    )
    publishedAt: str = Field(
        description="The published date of the news article."
    )
    news_outlet_source_id: str = Field(
        description="The news outlet source ID of the news article."
    )
    synctimestamp = Field(
        description="The timestamp of the sync."
    )
```

Now we can create our functions for inserting the articles into the database.

```python
def store_news_outlets_into_db() -> None:
    """Store the news outlets into the database.

    Should be initialized just once unless we add more sources.
    """
    res = []
    for party in political_party_to_news_outlets.keys():
        for news_outlet in political_party_to_news_outlets[party]:
            news_outlet_obj = {
                "outlet_id": news_outlet["id"],
                "domain": news_outlet["domain"],
                "political_party": party,
                "synctimestamp": current_datetime.strftime("%Y-%m-%d-%H:%M:%S") # noqa
            }
            NewsOutletModel(**news_outlet_obj)
            res.append(news_outlet_obj)
    bulk_insert_news_outlets(res)
    print(f"Inserted {len(res)} news outlets into the database.")


def store_latest_articles_into_db(latest_articles: dict) -> None:
    """Store the latest articles into the database."""
    res = []
    for party in latest_articles.keys():
        for news_source_articles_dict in latest_articles[party]:
            news_outlet_source_id = news_source_articles_dict["source"]
            articles = news_source_articles_dict["articles"]
            for article in articles:
                article_obj = {
                    "url": article["url"],
                    "title": article["title"],
                    "content": article["content"],
                    "description": article["description"],
                    "publishedAt": article["publishedAt"],
                    "news_outlet_source_id": news_outlet_source_id,
                    "synctimestamp": current_datetime.strftime("%Y-%m-%d-%H:%M:%S") # noqa
                }
                NewsArticleModel(**article_obj)
                res.append(article_obj)
    bulk_insert_news_articles(res)
    print(f"Inserted {len(res)} articles into the database.")

```

### Creating embeddings for outlets

We can create embeddings for each of the news articles, which will let us store vector representations of each news article so taht we can later do similarity matches. If we want to figure out what context to provide to a post, we need to have a way to figure out what the "most relevant" articles are.

The steps that I'll follow are:
- Create an embedding scheme.
- Convert the articles into Langchain Documents.
- Create vector stores for each political party and insert documents into vector stores
- Be able to query documents.

#### Creating an embedding scheme

For our embeddings, I'll use some out-of-the-box embeddings available via Hugging Face. There's a few other settings that I played around with, but I ended up with having enough settings to support a simple embedding model on CPU.
```python
DEFAULT_EMBEDDING_MODEL = "thenlper/gte-small"
print("Attempting to load embedding model.")
embedding_model = HuggingFaceEmbeddings(
    model_name=DEFAULT_EMBEDDING_MODEL,
    # multi_process=True,
    # model_kwargs={"device": "cuda"},
    # set True for cosine similarity
    encode_kwargs={"normalize_embeddings": True},
)
print("Embedding model loaded.")
```

#### Convert the articles into Langchain Documents
Next, I converted the articles into [Langchain Documents](https://python.langchain.com/docs/modules/data_connection/document_loaders/). Though not strictly necessary (since the actual unit that's embedded is just the text), converting to Documents allows us to add some nice metadata, which helps with tracking the article sources whenever we use them. We also enforce a maximum chunk size as well, for both our embedding model as well as for making sure that our context remains reasonably sized for our LLM.
```python
def convert_articles_to_documents(
    latest_articles_by_party: dict
) -> dict[str, list[LangchainDocument]]:
    """Convert a list of articles to a list of LangchainDocument objects.

    Combines the article title, content,  and description into one string.
    Takes a maximum chunk size so we don't make our documents too large,
    though this is less a problem since the NewsAPI service only returns
    previews of the articles.
    """
    political_party_to_knowledge_base: dict[str, list[LangchainDocument]] = {
        political_party: [
            LangchainDocument(
                page_content=(article["title"] + " " + article["content"] + " " + article["description"])[:MAX_CHUNK_SIZE], # noqa
                metadata={
                    "url": article["url"],
                    "publishedAt": article["publishedAt"],
                    "news_outlet_source_id": article["news_outlet_source_id"]
                }
            )
            for article in articles
        ]
        for (political_party, articles) in latest_articles_by_party.items()
    }
    return political_party_to_knowledge_base
```

#### Create vector stores for each political party and insert documents into vector stores

I want to be able to query documents for each political party so that I know what the political coverage for a given event has been for each party. This requires creating separate stores and indices for each party. Though technically vector stores and indices are different, Langchain and FAISS integrate them together. This is easily supported via Langchain and its FAISS integration. [FAISS](https://python.langchain.com/docs/integrations/vectorstores/faiss/) is a library created by Meta that allows for very fast embedding similarity search; this will let us do quick similarity searches between our query vector (the text that we want context for) and the articles in the database.

```python
current_directory = os.path.dirname(os.path.abspath(__file__))
MAX_CHUNK_SIZE = 512
DEFAULT_EMBEDDING_MODEL = "thenlper/gte-small"
VECTOR_DB_FOLDER_NAME = "current_events_indices"
FAISS_STORE_PATH = os.path.join(
    current_directory, VECTOR_DB_FOLDER_NAME
)
DEFAULT_FAISS_INDEX_NAME = "index" # name becomes "{name}.faiss"

political_party_to_index_fp_map = {
    "democrat": os.path.join(FAISS_STORE_PATH, "faiss_index_democrat"),
    "republican": os.path.join(FAISS_STORE_PATH, "faiss_index_republican"), # noqa
    "moderate": os.path.join(FAISS_STORE_PATH, "faiss_index_moderate"),
}

embedding_model = HuggingFaceEmbeddings(
    model_name=DEFAULT_EMBEDDING_MODEL,
    # multi_process=True,
    # model_kwargs={"device": "cuda"},
    # set True for cosine similarity
    encode_kwargs={"normalize_embeddings": True},
)

def insert_articles_into_vector_store(
    political_party_to_knowledge_base: dict[str, list[LangchainDocument]]
) -> None:
    """Create vector database and indices if they don't exist. Otherwise, loads
    the existing indices and updates them with new articles."""
    for party, articles in political_party_to_knowledge_base.items():
        index_path = political_party_to_index_fp_map[party]
        if not os.path.exists(FAISS_STORE_PATH):
            os.makedirs(FAISS_STORE_PATH)
        # create new vector store + index if necessary
        if not os.path.exists(index_path):
            print(f"Creating FAISS index for {party} at {index_path}...")
            faiss = FAISS.from_documents(
                documents=articles,
                embedding=embedding_model,
                distance_strategy=DistanceStrategy
            )
            # https://api.python.langchain.com/en/latest/_modules/langchain_community/vectorstores/faiss.html#FAISS.save_local # noqa
            # Can serialize if size is a problem:
            # https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.faiss.FAISS.html#langchain_community.vectorstores.faiss.FAISS.serialize_to_bytes # noqa
            # https://api.python.langchain.com/en/latest/_modules/langchain_community/vectorstores/faiss.html#FAISS.serialize_to_bytes # noqa
            faiss.save_local(
                folder_path=index_path,
                index_name=DEFAULT_FAISS_INDEX_NAME
            )
            print(f"Created and saved new FAISS index for {party} at {index_path}") # noqa
            print(f"Inserted {len(articles)} articles into the FAISS index for {party} at {index_path}") # noqa
        # use existing store + index
        else:
            vector_store: FAISS = FAISS.load_local(
                folder_path=index_path,
                embeddings=embedding_model,
                index_name=DEFAULT_FAISS_INDEX_NAME,
                allow_dangerous_deserialization=True
            )
            print(f"Loaded existing FAISS index for {party} at {index_path}")
            vector_store.add_documents(articles)
            print(f"Inserted {len(articles)} articles into the FAISS index for {party} at {index_path}") # noqa


def update_vector_store(latest_articles_by_party: dict[str, list[dict]]):
    """Update the vector store with the latest articles."""
    political_party_to_knowledge_base: dict[str, list[LangchainDocument]] = (
        convert_articles_to_documents(latest_articles_by_party)
    )
    insert_articles_into_vector_store(political_party_to_knowledge_base)
```

We can add our articles to our vector store like this:
```python
update_vector_store(latest_articles_by_party)
```

This now uploads all the articles synced from the NewsAPI service and saved to SQLite to now also be uploaded to the vector store.

#### Be able to query documents

Now that we have some documents available in the vector stores, we can query and see what news articles come up as a hit.

I want to see what each political party is saying about an event, so I can pass a query and then check against each index in order to get relevant documents.

```python
def query_political_party_index(
    political_party: str,
    query: str,
    top_n: Optional[int] = 5,
    similarity_distance: Optional[float] = None
) -> list[dict]:
    """Query the vector store for similar articles."""
    index_path: str = political_party_to_index_fp_map[political_party]
    vector_store: FAISS = FAISS.load_local(
        folder_path=index_path,
        embeddings=embedding_model,
        index_name=DEFAULT_FAISS_INDEX_NAME,
        # avoids value error from deserializing a pickle file. 
        allow_dangerous_deserialization=True
    )

    # in the future, can add filters to search: https://api.python.langchain.com/en/latest/vectorstores/langchain_community.vectorstores.faiss.FAISS.html#langchain_community.vectorstores.faiss.FAISS.similarity_search_by_vector # noqa
    if similarity_distance is not None:
        raise NotImplementedError("Querying by similarity distance not implemented yet.") # nqoa
    else:
        search_results: list[tuple[LangchainDocument], float] = (
            vector_store.similarity_search_with_score(query=query, k=top_n)
        )

    articles: list[dict] = []
    for result in search_results:
        document = result[0]
        score = result[1]
        # Format the result
        article_data = {
            "content": document.page_content,
            "metadata": {
                "url": document.metadata.get('url', 'URL not available'),
                "publishedAt": document.metadata.get('publishedAt', 'Date not available'), # noqa
                "news_outlet_source_id": document.metadata.get('news_outlet_source_id', 'Source ID not available'), # noqa
                "distance": score
            }
        }
        articles.append(article_data)
    return articles


def query_vector_store(
    query: str,
    top_n: Optional[int] = 5,
    similarity_distance: Optional[float] = None
) -> dict[str, list[dict]]:
    """Queries the vector store for similar articles.

    Given a particular query, searches each index and returns relevant
    articles from each of the indices. 
    """
    return {
        political_party: query_political_party_index(political_party, query, top_n, similarity_distance) # noqa
        for political_party in political_party_to_index_fp_map.keys()
    }
```

Now we can test the results:
```python
test_query = "I can't believe that these protests are happening."
search_results = query_vector_store(test_query)
```

Let's see what news outlets from different political parties have said about, for example, the pro-Palestinian college protests (see, for example, [this](https://www.cnn.com/2024/05/03/world/pro-palestinian-university-protests-worldwide-intl-hnk/index.html) CNN article for more information).

```python
epublican_coverage = [res["content"] for res in search_results["republican"][0:5]]
democrat_coverage = [res["content"] for res in search_results["democrat"][0:5]]
moderate_coverage = [res["content"] for res in search_results["moderate"][0:5]]
```

Republican coverage:

```plaintext
[
    '\'F*ck Joe Biden\' Chant Unites Pro-Palestinian and Counter-Protesters Pro-Palestinian protesters and counter-protesters at the University of Alabama in Tuscaloosa united to show their displeasure with President Joe Biden.\r\nVideo footage posted to X showed protesters on… [+2821 chars]
    Pro-Palestinian protesters and counter-protesters were united by chanting, "F*ck Joe Biden" at the University of Alabama.',
     
    'Police at UCLA face off against left-wing mob as nationwide anti-Israel protests escalate Police officers in riot gear have moved in on \r\nanti-Israel protesters at UCLA\'s campus.\xa0\r\nEarlier, law enforcement agencies in riot gear knocked down the plywood barrier surrounding the anti-Israel … [+865 chars] Police in riot gear are in a tense standoff with anti-Israel protesters at the encampment at UCLA and the school has switched to virtual classes amid an "emergency on campus." Anti-Israel protests continue on',
    
    'Pro-Palestinian Activists Rally at UCLA as Police Prepare to Remove Encampment Pro-Palestinian demonstrators rallied at the University of California Los Angeles (UCLA) on Wednesday evening as Los Angeles Police Department (LAPD) officers arrived in riot gear to remove their wee… [+3863 chars] Pro-Palestinian demonstrators rallied at UCLA as LAPD officers arrived in riot gear to remove the week-old "encampment."',
    
    'Fordham University: Police Raid Anti-Israel Encampment, Arrest 15 People Police raided an anti-Israel encampment at Fordham University this week, arresting 15 people.\r\nAs Breitbart News reported on Wednesday, an encampment of anti-Israel protesters sprung up at Fordham in… [+2820 chars] Police raided an anti-Israel encampment at Fordham University this week, arresting 15 people.',
    
    "WATCH LIVE: Police controlling crowd of anti-Israel agitators on UCLA quad after violent mob riots rocked campus | Fox News Video ©2024 FOX News Network, LLC. All rights reserved. This material may not be published, broadcast, rewritten, or redistributed. All market data delayed 20 minutes. Police intensify their crackdown of anti-Israel agitators on UCLA's campus as tensions rise ahead of an anticipated sweep of the encampment."
]
```

Democrat coverage:

```plaintext
[
    'US university protests live: UCLA campus on edge as riot police arrive blinking-dot\r\nLive updatesLive updates, \r\nPolice in riot gear deployed in force at the campus, ordering peaceful pro-Palestinian protesters to leave or face arrest. Police in riot gear deployed in force at the campus, ordering pro-Palestinian protesters to leave or face arrest.',
    
    'Live updates: Tensions mounting at UCLA; arrests made at Fordham as college protests spread Hundreds of protesters at the University of California at Los Angeles are packed against their encampment barricades, facing police in helmets and face shields standing shoulder to shoulder on the ot… [+308 chars] More universities across the country called in police to clear pro-Palestinian protesters demanding divestment from Israel, leading to arrests and confrontations.',
    
    'After weeks of college protests, police responses ramp up Colleges and universities reckoned Wednesday with the aftermath of major shows of police force across the country that cleared some protest encampments and emptied a Manhattan classroom building in a… [+437 chars] Even after a fragile calm resettled over campuses, footage of officers in riot gear sparked debates nationwide as Americans struggled to make sense of it all.',
    
    "Minneapolis is Burning: Protesters Loot and Set Buildings On Fire to Protest Police Killing of George Floyd Want the best of VICE News straight to your inbox? Sign up here.\r\n Minneapolis burned on Wednesday night as protesters clashed with police for a second night following the death of George Floyd after… [+4576 chars] Police used tear gas and rubber bullets on protesters who gathered outside the precinct where Derek Chauvin, the officer who kneeled on Floyd's neck until he was unresponsive, worked until ",
    
    'Tensions high at UCLA as police order pro-Palestinian protesters to leave Tensions are high at the University of California, Los Angeles (UCLA) campus where hundreds of police in riot gear have deployed in force, ordering peaceful pro-Palestinian protesters to leave or fac… [+5392 chars] Students staying put on US campus despite inevitability of ‘militarised police invasion’, ready to face arrest.'
]
```

Moderate coverage:

```plaintext
[
    'Carlotta Walls LaNier Babysat My Mom—Brown v. Board Was Watered Down In the winter of 1969, during the frigid spring semester at the University of Minnesota, a coalition of Black students staged a sit-in protest at Morrill Hall, occupying the office of the bursar and … [+10142 chars] Black students continue standing on the front lines of protests against police brutality and racism.',
    
    'New York City mayor: ‘It’s despicable’ schools would allow another country’s flag to fly in America Skip to content\r\nNew York City major Eric Adams said it was “despicable” that U.S. schools would allow another country’s flag to fly on their campuses while defending the New York Police Department (… [+1229 chars] New York City major Eric Adams said it was “despicable” that U.S. schools would allow another country’s flag to fly on their campuses while defending the New York Police Department (NYPD) after hun',
    
    'Marijuana\'s Reclassification Is Wrong America has recently been feeling the ill effects of one of the biggest "bait and switch" scams in our history. That is, the packaging of new marijuana laws as if they were just about decriminalizati… [+4352 chars] Somewhere along the line, our politicians and even top private industry leaders have abandoned any semblance of promoting an aspirational society.',
    
    'Global court in pressure cooker over threat of Israel arrest warrants Skip to content\r\nThe Biden administration and Israel’s supporters in Congress are lashing out at the International Criminal Court (ICC) over potential arrest warrants that could be issued for alleged… [+5787 chars] The Biden administration and Israel’s supporters in Congress are lashing out at the International Criminal Court (ICC) over potential arrest warrants that could be issued for alleged crimes by Israeli officials in prosecuting t',
    
    'Lawler, Moskowitz slam Greene over antisemitism bill pushback Skip to content\r\nReps. Mike Lawler (R-N.Y.) and Jared Moskowitz (D-Fla.) sharply criticized Rep. Marjorie Taylor Greene (R-Ga.) on Wednesday for her decision to oppose their antisemitism legislation,… [+3128 chars] Reps. Mike Lawler (R-N.Y.) and Jared Moskowitz (D-Fla.) sharply criticized Rep. Marjorie Taylor Greene (R-Ga.) on Wednesday for her decision to oppose their antisemitism legislation, and for her rationale for opposing it. Greene in a s'
]
```

There's a clear difference in how each of the political parties have covered the issue of the pro-Palestinian protests. We can provide this information to our model during its inference in order to give it "knowledge" of what's currently happening in the news, which should help it better classify posts that people have written about current events.

### Next steps
Next, I'll work on integrating this "current events context" service into a general LLM pipeline. Besides that, some other things to work on include:

- For determining when to get context for a post, investigate various strategies such as:
    - Keyword matching: see if a keyword (e.g., a name of an event) comes up. Need to figure out keywords that describe topics that are in the news (this is easiest if it is the name of a notable event, place, person, piece of legislature, etc.) and then we can easily pattern match that against posts that have that keyword.
    - Posts that the LLM knows is political but isn't sure what the political ideology is
- Only insert into the vector store if it doesn’t already exist there.
- At some point, add a maximum distance measure so we get only relevant articles (will take some experimentation in order to see what a good distance is).

Other things to work on after that are:
- How does our model perform with other LLMs (e.g., Mixtral)?
- Can we experiment with optimizing the prompt (e.g, with [dspy](https://github.com/stanfordnlp/dspy))?
