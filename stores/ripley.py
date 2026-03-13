import logging
from typing import Any, Dict

from bs4 import BeautifulSoup

from .base import (
    has_meaningful_page_data,
    scrape_with_beautifulsoup,
    scrape_with_playwright_crawler,
)

logger = logging.getLogger("api")


def extract_ripley_data(
    soup: BeautifulSoup, url: str, metadata: Dict[str, Any]
) -> Dict[str, Any]:
    # Agrega aqui los selectores especificos de Ripley.
    return {
        "url": url,
        "title": metadata.get("title") or (soup.title.string if soup.title else None),
        "h1s": [h1.text for h1 in soup.find_all("h1")],
        "h2s": [h2.text for h2 in soup.find_all("h2")],
        "h3s": [h3.text for h3 in soup.find_all("h3")],
        **metadata,
    }


async def scrape(url: str) -> Dict[str, Any]:
    soup_data = await scrape_with_beautifulsoup(url, extractor=extract_ripley_data)
    if has_meaningful_page_data(soup_data):
        logger.info(
            "🕷️ Ripley resuelto con BeautifulSoup | url=%s proxy=%s",
            url,
            soup_data.get("_proxy_used", "none"),
        )
        soup_data["_strategy"] = "beautifulsoup"
        return soup_data

    logger.warning(
        "⚠️ Ripley sin data util con BeautifulSoup, usando fallback PlaywrightCrawler | url=%s reason=%s proxy=%s",
        url,
        soup_data.get("_error", "empty_response"),
        soup_data.get("_proxy_used", "none"),
    )
    data = await scrape_with_playwright_crawler(url, extractor=extract_ripley_data)
    data["_strategy"] = "beautifulsoup->playwright-crawler"
    return data
