import asyncio
import json
import logging
import os
import time
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv

from crawlee.crawlers import (
    BasicCrawlingContext,
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
)
from crawlee.http_clients import ImpitHttpClient
from crawlee.proxy_configuration import ProxyConfiguration
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

load_dotenv()

logger = logging.getLogger("api")
SoupExtractor = Callable[[BeautifulSoup, str, Dict[str, Any]], Dict[str, Any]]

BEAUTIFULSOUP_PROXY_URLS = [
    url.strip()
    for url in os.getenv("BEAUTIFULSOUP_PROXY_URLS", "").split(",")
    if url.strip()
]
PLAYWRIGHT_PROXY_URLS = [
    url.strip()
    for url in os.getenv("PLAYWRIGHT_PROXY_URLS", "").split(",")
    if url.strip()
]
PLAYWRIGHT_CRAWLER_PROXY_URLS = [
    url.strip()
    for url in os.getenv("PLAYWRIGHT_CRAWLER_PROXY_URLS", "").split(",")
    if url.strip()
]
PLAYWRIGHT_PROXY_SERVER = os.getenv("PLAYWRIGHT_PROXY_SERVER", "").strip()
PLAYWRIGHT_PROXY_USERNAME = os.getenv("PLAYWRIGHT_PROXY_USERNAME", "").strip()
PLAYWRIGHT_PROXY_PASSWORD = os.getenv("PLAYWRIGHT_PROXY_PASSWORD", "").strip()
ENVIRONMENT = os.getenv("ENVIRONMENT", "").strip().lower()
SCRAPING_CONFIG_PATH = os.getenv("SCRAPING_CONFIG_PATH", "scraping_config.json").strip()


def load_scraping_config() -> Dict[str, Any]:
    default_config: Dict[str, Any] = {
        "defaults": {
            "beautifulsoup": {"use_proxy": True, "use_fast_http_client": True},
            "playwright": {"use_proxy": True},
            "playwright_crawler": {"use_proxy": True},
        }
    }
    try:
        with open(SCRAPING_CONFIG_PATH, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        logger.info("🗺️ Scraping config cargada | path=%s", SCRAPING_CONFIG_PATH)
        return loaded
    except Exception as exc:
        logger.warning(
            "⚠️ No se pudo cargar scraping config, usando defaults | path=%s error=%s",
            SCRAPING_CONFIG_PATH,
            exc,
        )
        return default_config


SCRAPING_CONFIG = load_scraping_config()


def get_strategy_settings(strategy: str) -> Dict[str, Any]:
    defaults = SCRAPING_CONFIG.get("defaults") or {}
    return dict(defaults.get(strategy) or {})


def get_store_strategy_settings(store: Optional[str], strategy: str) -> Dict[str, Any]:
    settings = get_strategy_settings(strategy)
    if not store:
        return settings
    store_config = ((SCRAPING_CONFIG.get("stores") or {}).get(store) or {})
    strategy_overrides = (store_config.get(strategy) or {})
    settings.update(strategy_overrides)
    return settings


def build_beautifulsoup_proxy_configuration(store: Optional[str] = None) -> Optional[ProxyConfiguration]:
    strategy_settings = get_store_strategy_settings(store, "beautifulsoup")
    if ENVIRONMENT == "dev":
        logger.info("🌐 BeautifulSoup proxy deshabilitado en ENVIRONMENT=dev")
        return None
    if not strategy_settings.get("use_proxy", True):
        logger.info("🌐 BeautifulSoup proxy deshabilitado por configuracion")
        return None
    if not BEAUTIFULSOUP_PROXY_URLS:
        logger.info("🌐 BeautifulSoup sin proxy configurado")
        return None
    logger.info(
        "🌐 BeautifulSoup proxies configurados | count=%s",
        len(BEAUTIFULSOUP_PROXY_URLS),
    )
    return ProxyConfiguration(proxy_urls=BEAUTIFULSOUP_PROXY_URLS)


def build_beautifulsoup_http_client(store: Optional[str] = None) -> Optional[ImpitHttpClient]:
    strategy_settings = get_store_strategy_settings(store, "beautifulsoup")
    if ENVIRONMENT == "dev":
        logger.info("🕷️ BeautifulSoup usando http client por defecto en ENVIRONMENT=dev")
        return None
    if not strategy_settings.get("use_fast_http_client", True):
        logger.info("🕷️ BeautifulSoup usando http client por defecto por configuracion")
        return None
    logger.info("🕷️ BeautifulSoup usando ImpitHttpClient con timeout=10s")
    return ImpitHttpClient(timeout=10)


def build_playwright_proxy_settings(store: Optional[str] = None) -> Optional[Dict[str, str]]:
    strategy_settings = get_store_strategy_settings(store, "playwright")
    if ENVIRONMENT == "dev":
        logger.info("🌐 Playwright proxy deshabilitado en ENVIRONMENT=dev")
        return None
    if not strategy_settings.get("use_proxy", True):
        logger.info("🌐 Playwright proxy deshabilitado por configuracion")
        return None
    if PLAYWRIGHT_PROXY_URLS:
        selected_proxy_url = PLAYWRIGHT_PROXY_URLS[0]
        logger.info(
            "🌐 Playwright proxy configurado desde lista | selected=%s count=%s",
            selected_proxy_url,
            len(PLAYWRIGHT_PROXY_URLS),
        )
        return {"server": selected_proxy_url}
    if not PLAYWRIGHT_PROXY_SERVER:
        logger.info("🌐 Playwright sin proxy configurado")
        return None
    proxy_settings = {"server": PLAYWRIGHT_PROXY_SERVER}
    if PLAYWRIGHT_PROXY_USERNAME:
        proxy_settings["username"] = PLAYWRIGHT_PROXY_USERNAME
    if PLAYWRIGHT_PROXY_PASSWORD:
        proxy_settings["password"] = PLAYWRIGHT_PROXY_PASSWORD
    logger.info(
        "🌐 Playwright proxy configurado | server=%s has_auth=%s",
        PLAYWRIGHT_PROXY_SERVER,
        bool(PLAYWRIGHT_PROXY_USERNAME or PLAYWRIGHT_PROXY_PASSWORD),
    )
    return proxy_settings


def build_playwright_crawler_proxy_configuration(store: Optional[str] = None) -> Optional[ProxyConfiguration]:
    strategy_settings = get_store_strategy_settings(store, "playwright_crawler")
    if ENVIRONMENT == "dev":
        logger.info("🌐 PlaywrightCrawler proxy deshabilitado en ENVIRONMENT=dev")
        return None
    if not strategy_settings.get("use_proxy", True):
        logger.info("🌐 PlaywrightCrawler proxy deshabilitado por configuracion")
        return None
    if not PLAYWRIGHT_CRAWLER_PROXY_URLS:
        logger.info("🌐 PlaywrightCrawler sin lista de proxies configurada")
        return None
    logger.info(
        "🌐 PlaywrightCrawler proxies configurados | count=%s",
        len(PLAYWRIGHT_CRAWLER_PROXY_URLS),
    )
    return ProxyConfiguration(proxy_urls=PLAYWRIGHT_CRAWLER_PROXY_URLS)


def has_meaningful_page_data(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("title"):
        return True
    if data.get("h1s") or data.get("h2s") or data.get("h3s"):
        return True
    return False


def default_soup_extractor(
    soup: BeautifulSoup, url: str, metadata: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "url": url,
        "title": soup.title.string if soup.title else None,
        "h1s": [h1.text for h1 in soup.find_all("h1")],
        "h2s": [h2.text for h2 in soup.find_all("h2")],
        "h3s": [h3.text for h3 in soup.find_all("h3")],
        **metadata,
    }


async def scrape_with_beautifulsoup(
    url: str, extractor: Optional[SoupExtractor] = None, store: Optional[str] = None
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("🕷️ Estrategia BeautifulSoupCrawler | url=%s", url)
    result: Dict[str, Any] = {
        "url": url,
        "title": None,
        "h1s": [],
        "h2s": [],
        "h3s": [],
        "_proxy_used": "none",
        "_error": None,
    }
    crawler = BeautifulSoupCrawler(
        max_request_retries=1,
        request_handler_timeout=timedelta(seconds=30),
        max_requests_per_crawl=1,
        proxy_configuration=build_beautifulsoup_proxy_configuration(store),
        http_client=build_beautifulsoup_http_client(store),
    )
    done = asyncio.get_running_loop().create_future()
    attempt_counter = {"count": 0}

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        attempt_counter["count"] += 1
        proxy_url = getattr(getattr(context, "proxy_info", None), "url", None)
        logger.info(
            "🕷️ BeautifulSoup procesando respuesta | url=%s attempt=%s proxy=%s",
            context.request.url,
            attempt_counter["count"],
            proxy_url or "none",
        )
        html_preview = str(context.soup)[:500].replace("\n", " ").replace("\r", " ")
        logger.info(
            "🕷️ BeautifulSoup html preview | url=%s html_500=%s",
            context.request.url,
            html_preview,
        )
        parsed = (extractor or default_soup_extractor)(
            context.soup,
            context.request.url,
            {"_proxy_used": proxy_url or "none", "_error": None},
        )
        if not done.done():
            done.set_result(parsed)

    @crawler.pre_navigation_hook
    async def _hook(context: BasicCrawlingContext) -> None:
        _ = context

    try:
        await crawler.run([url])
        elapsed = time.perf_counter() - started_at
        logger.info(
            "🕷️ BeautifulSoupCrawler completado | url=%s attempts=%s elapsed=%.2fs",
            url,
            attempt_counter["count"],
            elapsed,
        )
        if not done.done():
            result["_error"] = "no_response_processed"
            logger.warning(
                "⚠️ BeautifulSoupCrawler termino sin procesar respuesta | url=%s attempts=%s elapsed=%.2fs",
                url,
                attempt_counter["count"],
                elapsed,
            )
            done.set_result(result)
    except asyncio.CancelledError:
        elapsed = time.perf_counter() - started_at
        result["_error"] = "cancelled"
        logger.warning(
            "⚠️ BeautifulSoupCrawler cancelado | url=%s attempts=%s elapsed=%.2fs",
            url,
            attempt_counter["count"],
            elapsed,
        )
        if not done.done():
            done.set_result(result)
    except Exception as exc:
        elapsed = time.perf_counter() - started_at
        error_name = exc.__class__.__name__.lower()
        result["_error"] = error_name or "beautifulsoup_error"
        logger.warning("⚠️ BeautifulSoupCrawler fallo para %s: %s", url, exc)
        logger.warning(
            "⚠️ BeautifulSoupCrawler duracion con fallo | url=%s attempts=%s elapsed=%.2fs",
            url,
            attempt_counter["count"],
            elapsed,
        )
        if not done.done():
            done.set_result(result)

    if done.done():
        return done.result()
    return result


async def scrape_with_playwright(
    url: str, extractor: Optional[SoupExtractor] = None, store: Optional[str] = None
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("🎭 Estrategia Playwright | url=%s", url)
    proxy_settings = build_playwright_proxy_settings(store)
    logger.info(
        "🎭 Playwright usando proxy | url=%s proxy=%s",
        url,
        (proxy_settings or {}).get("server", "none"),
    )
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
            proxy=proxy_settings,
        )
        context = await browser.new_context(
            locale="es-CL",
            timezone_id="America/Santiago",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            extra_http_headers={"accept-language": "es-CL,es;q=0.9,en;q=0.8"},
        )
        page = await context.new_page()
        await page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['es-CL', 'es', 'en'] });
            Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
            """
        )

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3500)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            html_preview = html[:500].replace("\n", " ").replace("\r", " ")
            logger.info("🎭 Playwright html preview | url=%s html_500=%s", url, html_preview)
            elapsed = time.perf_counter() - started_at
            logger.info("🎭 Playwright cargo pagina | url=%s elapsed=%.2fs", url, elapsed)
            return (extractor or default_soup_extractor)(
                soup,
                url,
                {"title": await page.title()},
            )
        except PlaywrightTimeoutError as exc:
            elapsed = time.perf_counter() - started_at
            logger.warning("⚠️ Playwright timeout para %s: %s", url, exc)
            logger.warning("⚠️ Playwright duracion con timeout | url=%s elapsed=%.2fs", url, elapsed)
            return {
                "url": url,
                "title": None,
                "h1s": [],
                "h2s": [],
                "h3s": [],
            }
        finally:
            await context.close()
            await browser.close()


async def scrape_with_playwright_crawler(
    url: str, extractor: Optional[SoupExtractor] = None, store: Optional[str] = None
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("🎭 Estrategia PlaywrightCrawler | url=%s", url)
    proxy_configuration = build_playwright_crawler_proxy_configuration(store)
    result: Dict[str, Any] = {
        "url": url,
        "title": None,
        "h1s": [],
        "h2s": [],
        "h3s": [],
    }
    crawler = PlaywrightCrawler(
        max_request_retries=1,
        request_handler_timeout=timedelta(seconds=60),
        max_requests_per_crawl=1,
        browser_type="chromium",
        headless=True,
        browser_launch_options={
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        },
        browser_new_context_options={
            "locale": "es-CL",
            "timezone_id": "America/Santiago",
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "viewport": {"width": 1366, "height": 768},
            "extra_http_headers": {"accept-language": "es-CL,es;q=0.9,en;q=0.8"},
        },
        proxy_configuration=proxy_configuration,
    )
    done = asyncio.get_running_loop().create_future()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        proxy_url = getattr(getattr(context, "proxy_info", None), "url", None)
        logger.info(
            "🎭 PlaywrightCrawler procesando respuesta | url=%s proxy=%s",
            context.request.url,
            proxy_url or "none",
        )
        await context.page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['es-CL', 'es', 'en'] });
            Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
            """
        )
        html = await context.page.content()
        soup = BeautifulSoup(html, "html.parser")
        html_preview = html[:500].replace("\n", " ").replace("\r", " ")
        logger.info(
            "🎭 PlaywrightCrawler html preview | url=%s html_500=%s",
            context.request.url,
            html_preview,
        )
        parsed = (extractor or default_soup_extractor)(
            soup,
            context.request.url,
            {
                "title": await context.page.title(),
                "_proxy_used": proxy_url or "none",
            },
        )
        if not done.done():
            done.set_result(parsed)

    try:
        await crawler.run([url])
        elapsed = time.perf_counter() - started_at
        logger.info(
            "🎭 PlaywrightCrawler completado | url=%s elapsed=%.2fs",
            url,
            elapsed,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started_at
        logger.warning("⚠️ PlaywrightCrawler fallo para %s: %s", url, exc)
        logger.warning(
            "⚠️ PlaywrightCrawler duracion con fallo | url=%s elapsed=%.2fs",
            url,
            elapsed,
        )
        if not done.done():
            done.set_result(result)

    if done.done():
        return done.result()
    return result
