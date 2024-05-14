# Experiments with LLM web scraping

## Problem setup
I already wrote a [proof-of-concept](https://markptorres.com/research/llm-experiments-pt-iii) for creating a RAG-based app for summarizing news content and adding that as context to an LLM. However, we are inherently limited if we only have the preview of the news article and some of the key highlights.

There are a few things that writing a web scraper and fetching longer-form articles helps us accomplish:
- We can analyze the articles and extract relevant topics. We can learn about "trending topics" over time.
- We can build knowledge graphs to learn more about how different articles relate to each other.
- Given that we can create more topics, we can also build more robust summaries about what different political parties think about a certain topic.
- We can bypass limitations in the [NewsAPI](https://newsapi.org/docs/client-libraries/python) service, which'll give us only a subset of news providers, and fetch data from more news providers as well.


## Attempt 1: Setting up an LLM web scraper
I'd prefer to not have to write a manual web scraper though. Luckily, LLM scrapers are a popular use case of LLMs now, so there are a few options to explore. I'm going to use [ScrapeGraphAI](https://github.com/VinciGit00/Scrapegraph-ai), a library that "uses LLM and direct graph logic to create scraping pipelines for websites and local documents (XML, HTML, JSON, etc.).", as per the company's Github page.

### Installing 
First, we install the package:

```bash
pip install scrapegraphai
```

Then, we install `playwright`:

```bash
playwright install
```

### Initial setup
I'll use Gemini as the backend for this web scraper. I'll first start with a demo example from the package's Github repo, just to see if it works:

```python
from scrapegraphai.graphs import SmartScraperGraph

from lib.helper import GOOGLE_AI_STUDIO_KEY

api_key = GOOGLE_AI_STUDIO_KEY

graph_config = {
    "llm": {
        "model": "gemini-pro",
        "api_key": api_key,
        "temperature": 0
    },
    "verbose": True,
    "max_results": 5
}


smart_scraper_graph = SmartScraperGraph(
    prompt="List me all the projects with their descriptions",
    # also accepts a string with the already downloaded HTML code
    source="https://perinim.github.io/projects",
    config=graph_config
)

result = smart_scraper_graph.run()
print(result)
```

We get the following results, which look good!
```plaintext
Processing chunks: 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00, 1242.02it/s]
{'projects': [{'name': 'Rotary Pendulum RL', 'description': 'Open Source project aimed at controlling a real life rotary pendulum using RL algorithms'}, {'name': 'DQN Implementation from scratch', 'description': 'Developed a Deep Q-Network algorithm to train a simple and double pendulum'}, {'name': 'Multi Agents HAED', 'description': 'University project which focuses on simulating a multi-agent system to perform environment mapping. Agents, equipped with sensors, explore and record their surroundings, considering uncertainties in their readings.'}, {'name': 'Wireless ESC for Modular Drones', 'description': 'Modular drone architecture proposal and proof of concept. The project received maximum grade.'}]}
```

### Trying it for a sample news org
I then tried it for on a CNN article:

```python
def get_all_article_content(url: str):
    smart_scraper_graph = SmartScraperGraph(
        prompt = "What is this about?",
        source=url,
        config=graph_config
    )
    result = smart_scraper_graph.run()
    return result


if __name__ == "__main__":
    url = "https://edition.cnn.com/2024/05/13/politics/louisiana-bill-abortion-drugs-highly-regulated-medications/index.html"
    result = get_all_article_content(url)
    breakpoint()
```

Checking the results, I get:
```plaintext
(Pdb) result
{'What is this about?': 'A Louisiana bill would classify the abortion-inducing drugs misoprostol and mifeprostone as Schedule IV controlled dangerous substances in the state, placing them in the same category as highly regulated drugs such as narcotics and depressants.'}
```

However, if I want the specific text, I don't get a good result:
```python
def get_all_article_content(url: str):
    smart_scraper_graph = SmartScraperGraph(
        prompt="Return the text of the article.",
        source=url,
        config=graph_config
    )
    result = smart_scraper_graph.run()
    return result
```

When we ask for the text of the article, we don't get the actual text back:
```plaintext

```

I did further testing and found that this was true when we prompted the LLM with specific tags and labels in the DOM.

It looks like this particular library is good for answering specific questions about a web page. [Other](https://github.com/mendableai/firecrawl) LLM scrapers do a similar task. Instead of grabbing the exact text on the page itself, it lets you answer questions about the text.

## Attempt 2: Having an LLM generate the web scraping code
Instead of using an LLM scraper, what if I had an LLM help me write a web scraper instead?

