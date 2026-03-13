from typing import Any, Dict

from bs4 import BeautifulSoup

from .base import scrape_with_beautifulsoup


def extract_paris_data(
    soup: BeautifulSoup, url: str, metadata: Dict[str, Any]
) -> Dict[str, Any]:
    # Agrega aqui los selectores especificos de Paris.
    return {
        "url": url,
        "title": metadata.get("title") or (soup.title.string if soup.title else None),
        "h1s": [h1.text for h1 in soup.find_all("h1")],
        "h2s": [h2.text for h2 in soup.find_all("h2")],
        "h3s": [h3.text for h3 in soup.find_all("h3")],
        **metadata,
    }


async def scrape(url: str) -> Dict[str, Any]:
    data = await scrape_with_beautifulsoup(url, extractor=extract_paris_data)
    data["_strategy"] = "beautifulsoup"
    return data
