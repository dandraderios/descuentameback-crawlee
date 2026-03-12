import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

from crawlee.crawlers import (
    BasicCrawlingContext,
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

SUPPORTED_STORES = {"falabella", "meli", "mercadolibre", "paris", "ripley"}
PRICE_CHECKER_TOKEN = os.getenv("PRICE_CHECKER_TOKEN", "").strip()


class ProductPriceCheckRequest(BaseModel):
    batch_size: int = Field(1, ge=1, le=50)
    store: Optional[str] = None
    product_id: Optional[str] = None


class MongoDB:
    def __init__(self) -> None:
        self.client: Optional[MongoClient] = None
        self.db = None
        self.products: Optional[Collection] = None
        self.notifications: Optional[Collection] = None
        self.connect()

    def connect(self) -> None:
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise ValueError("MONGODB_URI no está configurada")

        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client.get_default_database()
            self.products = self.db.products
            self.notifications = self.db.notifications
            logger.info("✅ Conectado a MongoDB")
        except ConnectionFailure as exc:
            logger.error("❌ Error conectando a MongoDB: %s", exc)
            raise

    def close(self) -> None:
        if self.client is not None:
            self.client.close()


mongo_db = MongoDB()
app = FastAPI(title="Descuentame Price Checker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_store(raw_store: Optional[str]) -> str:
    store = (raw_store or "").strip().lower()
    if store in {"mercadolibre", "mercado-libre", "ml", "mlc"}:
        return "meli"
    return store


def build_store_query_values(raw_store: Optional[str]) -> List[str]:
    normalized_store = normalize_store(raw_store)
    if normalized_store == "meli":
        return ["meli", "mercadolibre", "mercado-libre", "ml", "mlc"]
    return [normalized_store] if normalized_store else []


def require_internal_token(x_internal_token: Optional[str] = Header(default=None)) -> None:
    if not PRICE_CHECKER_TOKEN:
        return
    if x_internal_token != PRICE_CHECKER_TOKEN:
        raise HTTPException(status_code=401, detail="x-internal-token invalido")


def has_meaningful_page_data(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("title"):
        return True
    if data.get("h1s") or data.get("h2s") or data.get("h3s"):
        return True
    return False


async def scrape_with_beautifulsoup(url: str) -> Dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("🕷️ Estrategia BeautifulSoupCrawler | url=%s", url)
    result: Dict[str, Any] = {
        "url": url,
        "title": None,
        "h1s": [],
        "h2s": [],
        "h3s": [],
    }
    crawler = BeautifulSoupCrawler(
        max_request_retries=1,
        request_handler_timeout=timedelta(seconds=30),
        max_requests_per_crawl=1,
    )
    done = asyncio.get_running_loop().create_future()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        logger.info("🕷️ BeautifulSoup procesando respuesta | url=%s", context.request.url)
        html_preview = str(context.soup)[:500].replace("\n", " ").replace("\r", " ")
        logger.info(
            "🕷️ BeautifulSoup html preview | url=%s html_500=%s",
            context.request.url,
            html_preview,
        )
        parsed = {
            "url": context.request.url,
            "title": context.soup.title.string if context.soup.title else None,
            "h1s": [h1.text for h1 in context.soup.find_all("h1")],
            "h2s": [h2.text for h2 in context.soup.find_all("h2")],
            "h3s": [h3.text for h3 in context.soup.find_all("h3")],
        }
        if not done.done():
            done.set_result(parsed)

    @crawler.pre_navigation_hook
    async def _hook(context: BasicCrawlingContext) -> None:
        _ = context

    try:
        await crawler.run([url])
        elapsed = time.perf_counter() - started_at
        logger.info("🕷️ BeautifulSoupCrawler completado | url=%s elapsed=%.2fs", url, elapsed)
    except Exception as exc:
        elapsed = time.perf_counter() - started_at
        logger.warning("⚠️ BeautifulSoupCrawler fallo para %s: %s", url, exc)
        logger.warning("⚠️ BeautifulSoupCrawler duracion con fallo | url=%s elapsed=%.2fs", url, elapsed)
        if not done.done():
            done.set_result(result)

    if done.done():
        return done.result()
    return result


async def scrape_with_playwright(url: str) -> Dict[str, Any]:
    started_at = time.perf_counter()
    logger.info("🎭 Estrategia Playwright | url=%s", url)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
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
            return {
                "url": url,
                "title": await page.title(),
                "h1s": [h1.text for h1 in soup.find_all("h1")],
                "h2s": [h2.text for h2 in soup.find_all("h2")],
                "h3s": [h3.text for h3 in soup.find_all("h3")],
            }
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


async def scrape_current_page_data(store_id: str, product_url: str) -> Dict[str, Any]:
    normalized_store = normalize_store(store_id)
    strategy = "playwright" if normalized_store == "meli" else "beautifulsoup"
    logger.info(
        "🔀 Estrategia seleccionada | store=%s normalized_store=%s strategy=%s url=%s",
        store_id,
        normalized_store,
        strategy,
        product_url,
    )
    if normalized_store == "meli":
        data = await scrape_with_playwright(product_url)
        data["_strategy"] = "playwright"
        return data
    if normalized_store == "ripley":
        soup_data = await scrape_with_beautifulsoup(product_url)
        if has_meaningful_page_data(soup_data):
            logger.info(
                "🕷️ Ripley resuelto con BeautifulSoup | url=%s",
                product_url,
            )
            soup_data["_strategy"] = "beautifulsoup"
            return soup_data
        logger.warning(
            "⚠️ Ripley sin data util con BeautifulSoup, usando fallback Playwright | url=%s",
            product_url,
        )
        data = await scrape_with_playwright(product_url)
        data["_strategy"] = "beautifulsoup->playwright"
        return data
    if normalized_store in {"falabella", "paris"}:
        data = await scrape_with_beautifulsoup(product_url)
        data["_strategy"] = "beautifulsoup"
        return data
    return {
        "url": product_url,
        "title": None,
        "h1s": [],
        "h2s": [],
        "h3s": [],
        "_strategy": "unsupported",
    }


async def run_price_checker_sync(request: ProductPriceCheckRequest) -> Dict[str, Any]:
    request_started_at = time.perf_counter()
    if mongo_db.products is None:
        raise HTTPException(status_code=500, detail="MongoDB no disponible")

    requested_store = normalize_store(request.store)
    requested_store_values = build_store_query_values(request.store)
    requested_product_id = (request.product_id or "").strip()

    query: Dict[str, Any] = {"status": {"$ne": "archived"}}
    if requested_store:
        if requested_store not in SUPPORTED_STORES:
            return {
                "summary": {
                    "processed": 0,
                    "vigente": 0,
                    "expirado": 0,
                    "no_checkeado": 0,
                    "not_implemented": 1,
                },
                "results": [
                    {
                        "store": requested_store,
                        "message": "price-check no implementado para esta tienda",
                    }
                ],
            }
        query["store.store_id"] = {"$in": requested_store_values}
    else:
        query["store.store_id"] = {
            "$in": sorted(
                {
                    "falabella",
                    "paris",
                    "ripley",
                    "meli",
                    "mercadolibre",
                }
            )
        }

    if requested_product_id:
        query["product_id"] = requested_product_id

    projection = {
        "product_id": 1,
        "product_name": 1,
        "store": 1,
        "link_market": 1,
        "prices.current_price": 1,
        "prices.old_price": 1,
        "prices.cmr_price": 1,
        "prices.card_price": 1,
        "prices.cenco_card_price": 1,
        "prices.ripley_card_price": 1,
        "prices.discount": 1,
        "created_at": 1,
        "status": 1,
    }
    limit = 1 if requested_product_id else request.batch_size
    products = list(
        mongo_db.products.find(query, projection)
        .sort([("price_checked_at", 1), ("created_at", 1)])
        .limit(limit)
    )

    if not products:
        return {"summary": {"processed": 0, "vigente": 0, "expirado": 0, "no_checkeado": 0, "not_implemented": 0}, "results": []}

    results: List[Dict[str, Any]] = []

    for product in products:
        product_started_at = time.perf_counter()
        product_id = str(product.get("product_id") or "")
        store_id = normalize_store(((product.get("store") or {}).get("store_id") or "").strip())
        product_url = (product.get("link_market") or "").strip()
        logger.info(
            "🚀 Iniciando scraping | product_id=%s store=%s url=%s",
            product_id,
            store_id,
            product_url,
        )

        result_item: Dict[str, Any] = {
            "product_id": product_id,
            "store": store_id,
            "product_url": product_url,
            "data": None,
        }

        try:
            if not product_url or not store_id:
                raise ValueError("Producto sin URL o store_id")

            result_item["data"] = await scrape_current_page_data(store_id, product_url)
            elapsed = time.perf_counter() - product_started_at
            logger.info(
                "✅ Scraping completado | product_id=%s store=%s strategy=%s elapsed=%.2fs",
                product_id,
                store_id,
                (result_item["data"] or {}).get("_strategy", "unknown"),
                elapsed,
            )
        except Exception as exc:
            elapsed = time.perf_counter() - product_started_at
            result_item["error"] = str(exc)
            logger.warning(
                "⚠️ Scraping fallo | product_id=%s store=%s error=%s elapsed=%.2fs",
                product_id,
                store_id,
                exc,
                elapsed,
            )

        results.append(result_item)

    summary = {
        "processed": len(results),
        "success": sum(1 for item in results if item.get("data")),
        "errors": sum(1 for item in results if item.get("error")),
        "not_implemented": 0,
    }
    total_elapsed = time.perf_counter() - request_started_at
    logger.info(
        "🏁 Price-check request completado | processed=%s success=%s errors=%s elapsed=%.2fs",
        summary["processed"],
        summary["success"],
        summary["errors"],
        total_elapsed,
    )
    return {"summary": summary, "results": results}


@app.get("/health/check")
async def health_check() -> Dict[str, Any]:
    return {
        "success": True,
        "data": {
            "status": "healthy",
            "db": "connected" if mongo_db.client else "disconnected",
            "timestamp": datetime.now().timestamp(),
        },
        "error": None,
        "message": "Health check",
    }


@app.post("/api/v1/products/price-check/run")
async def run_price_checker(
    request: ProductPriceCheckRequest,
    x_internal_token: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_internal_token(x_internal_token)
    data = await run_price_checker_sync(request)
    return {
        "success": True,
        "message": "Price-check ejecutado",
        "data": data,
    }


@app.on_event("shutdown")
def shutdown_event() -> None:
    mongo_db.close()
