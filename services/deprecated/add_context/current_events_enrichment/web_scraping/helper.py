"""Scrapes web pages using ScrapeGraph

GitHub repo: https://github.com/VinciGit00/Scrapegraph-ai
"""

import requests

from lib.load_env_vars import EnvVarsContainer
from ml_tooling.llm.inference import run_query

api_key = EnvVarsContainer.get_env_var("GOOGLE_AI_STUDIO_KEY") or ""


def get_article_contents(url: str):
    response = requests.get(url)
    html_content = response.text
    return html_content


def get_article_content_llm(url: str):
    html_content: str = get_article_contents(url)
    prompt = f"""Given the following HTML content, return the main
    text of the news article. Return as a JSON with key = "content"
    and value = the main text of the article, as a string.

    {html_content}"""
    # token count: 800,000
    result = run_query(prompt)
    return result


if __name__ == "__main__":
    url = "https://edition.cnn.com/2024/05/13/politics/louisiana-bill-abortion-drugs-highly-regulated-medications/index.html"  # noqa
    # result = get_all_article_content(url)
    result = get_article_content_llm(url)


# smart_scraper_graph = SmartScraperGraph(
#     prompt="List me all the projects with their descriptions",
#     # also accepts a string with the already downloaded HTML code
#     source="https://perinim.github.io/projects",
#     config=graph_config
# )

# result = smart_scraper_graph.run()
# print(result)
