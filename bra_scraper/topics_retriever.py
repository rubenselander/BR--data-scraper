import requests
import urllib3
from lxml import html
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# URLs
BASE_URL = "https://statistik.bra.se/solwebb/action/"
CATALOG_URL = "https://statistik.bra.se/solwebb/action/start?menykatalogid=1"
TOPIC_URL_TEMPLATE = (
    "https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={}"
)
topics_regex_pattern = r'<li class="menySol">.*?menyid=(\d+).*?class="menytext">(.*?)<\/span>.*?<li class="menyText">(.*?)<\/li>'


def _get_catalog_page() -> str:
    """Fetches the page with all topics."""
    response = requests.get(CATALOG_URL, verify=False)
    return response.text


def _extract_topics(html_content: str) -> list[dict[str, str]]:
    """Extracts the topics from the HTML content.
    :param html_content (str): The HTML content of the topics page.
    :returns (list): A list of topics as dictionaries.
    """
    topics = []

    for match in re.findall(topics_regex_pattern, html_content, re.DOTALL):
        id, name_html, description = match
        # Clean up the name by removing HTML entities and tags
        name = re.sub("<[^>]+>", "", name_html).replace("&nbsp;", " ").strip()
        description = description.strip()
        topic = {"id": id, "name": name, "description": description}
        topics.append(topic)

    return topics


def get_topics() -> list[dict]:
    """Fetches the topics from the website.
    :returns (list): A list of topics as dictionaries.
    """
    page = _get_catalog_page()
    topics = _extract_topics(page)
    return sorted(topics, key=lambda x: int(x["id"]))


def get_topic_page(topic_id: str) -> str:
    """Fetches the page for a specific topic.
    :param topic_id (str): The ID of the topic.
    :returns (str): The HTML content of the page.
    """
    with requests.Session() as session:
        session.get(CATALOG_URL, verify=False)
        response = session.get(TOPIC_URL_TEMPLATE.format(topic_id), verify=False)
    return response.text
