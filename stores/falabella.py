import re
from typing import Any, Dict, List

from bs4 import BeautifulSoup

from .base import (
    has_meaningful_page_data,
    scrape_with_beautifulsoup,
    scrape_with_playwright_crawler,
)


def extract_falabella_data(
    soup: BeautifulSoup, url: str, metadata: Dict[str, Any]
) -> Dict[str, Any]:
    price_like_elements = []
    for element in soup.find_all(True):
        tag_name = (element.name or "").lower()
        classes = [cls for cls in (element.get("class") or []) if isinstance(cls, str)]
        class_text = " ".join(classes).lower()
        if "price" not in tag_name and "price" not in class_text:
            continue

        text = element.get_text(" ", strip=True)
        if not text:
            continue

        price_like_elements.append(
            {
                "tag": tag_name,
                "classes": classes,
                "text": text,
            }
        )

    prices: Dict[str, Any] = {
        "cmr_price": None,
        "current_price": None,
        "old_price": None,
        "discount": None,
    }
    selected_price_block = None
    for item in price_like_elements:
        if "prices-container" in item["classes"]:
            selected_price_block = item["text"]
            break

    if selected_price_block:
        found_prices: List[str] = re.findall(r"\$\s*[\d\.]+", selected_price_block)
        found_discount = re.search(r"-\s*(\d+)%", selected_price_block)

        normalized_prices = [re.sub(r"\s+", " ", price).replace("$ ", "$").strip() for price in found_prices]
        if len(normalized_prices) >= 3:
            prices["cmr_price"] = normalized_prices[0]
            prices["current_price"] = normalized_prices[1]
            prices["old_price"] = normalized_prices[2]
        elif len(normalized_prices) >= 2:
            prices["current_price"] = normalized_prices[0]
            prices["old_price"] = normalized_prices[1]
        elif len(normalized_prices) == 1:
            prices["current_price"] = normalized_prices[0]

        if found_discount:
            prices["discount"] = found_discount.group(1)

    return {
        "url": url,
        "title": metadata.get("title") or (soup.title.string if soup.title else None),
        "h1s": [h1.text for h1 in soup.find_all("h1")],
        "h2s": [h2.text for h2 in soup.find_all("h2")],
        "h3s": [h3.text for h3 in soup.find_all("h3")],
        "price_like_elements": price_like_elements,
        "prices": prices,
        **metadata,
    }


async def scrape(url: str) -> Dict[str, Any]:
    data = await scrape_with_beautifulsoup(url, extractor=extract_falabella_data)
    if has_meaningful_page_data(data):
        data["_strategy"] = "beautifulsoup"
        return data

    data = await scrape_with_playwright_crawler(url, extractor=extract_falabella_data)
    data["_strategy"] = "beautifulsoup->playwright-crawler"
    return data
